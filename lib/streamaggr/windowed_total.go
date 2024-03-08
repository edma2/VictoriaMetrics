package streamaggr

import (
	"math"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
)

// windowedTotalAggrState calculates output=total, e.g. the summary counter over input counters.
type windowedTotalAggrState struct {
	m sync.Map

	suffix string

	// Whether to reset the output value on every flushState call.
	resetTotalOnFlush bool

	// Whether to take into account the first sample in new time series when calculating the output value.
	keepFirstSample bool

	// The time interval
	intervalSecs uint64

	// Time series state is dropped if no new samples are received during stalenessSecs.
	//
	// Aslo, the first sample per each new series is ignored during stalenessSecs even if keepFirstSample is set.
	// see ignoreFirstSampleDeadline for more details.
	stalenessSecs uint64

	// The first sample per each new series is ignored until this unix timestamp deadline in seconds even if keepFirstSample is set.
	// This allows avoiding an initial spike of the output values at startup when new time series
	// cannot be distinguished from already existing series. This is tracked with ignoreFirstSampleDeadline.
	ignoreFirstSampleDeadline uint64

	// Ignore samples which were older than the last seen sample. This is preferable to treating it as a
	// counter reset.
	ignoreOutOfOrderSamples bool
}

type windowedTotalStateValue struct {
	mu             sync.Mutex
	lastValues     map[string]windowedLastValueState
	windows        []window // from oldest ... newest
	total          float64
	deleteDeadline uint64
	deleted        bool
}

type window struct {
	endTimestamp int64
	delta        float64
}

type windowedLastValueState struct {
	value          float64
	timestamp      int64
	deleteDeadline uint64
}

func newWindowedTotalAggrState(interval time.Duration, stalenessInterval time.Duration, resetTotalOnFlush, keepFirstSample bool, ignoreOutOfOrderSamples bool) *windowedTotalAggrState {
	stalenessSecs := roundDurationToSecs(stalenessInterval)
	intervalSecs := roundDurationToSecs(interval)
	ignoreFirstSampleDeadline := fasttime.UnixTimestamp() + stalenessSecs
	suffix := "total"
	if resetTotalOnFlush {
		suffix = "increase"
	}
	return &windowedTotalAggrState{
		suffix:                    suffix,
		resetTotalOnFlush:         resetTotalOnFlush,
		keepFirstSample:           keepFirstSample,
		intervalSecs:              intervalSecs,
		stalenessSecs:             stalenessSecs,
		ignoreFirstSampleDeadline: ignoreFirstSampleDeadline,
		ignoreOutOfOrderSamples:   ignoreOutOfOrderSamples,
	}
}

// roundUp rounds up n to the nearest r.
func roundUp(n, r int64) int64 {
	if n%r == 0 {
		return n
	}
	return r - (n % r) + n
}

func (as *windowedTotalAggrState) pushDelta(sv *windowedTotalStateValue, delta float64, timestamp int64) {
	windowTimestamp := roundUp(timestamp, int64(as.intervalSecs)*1000)

	// no windows yet, create a new window
	if len(sv.windows) == 0 {
		sv.windows = []window{{windowTimestamp, delta}}
		return
	}

	// check latest window
	w := sv.windows[len(sv.windows)-1]
	if windowTimestamp == w.endTimestamp {
		// window found
		w.delta += delta
	} else if windowTimestamp > w.endTimestamp {
		// window needs to be created
		w := window{windowTimestamp, delta}
		sv.windows = append(sv.windows, w)
	} else {
		// otherwise, check older windows
		for _, w := range sv.windows[:len(sv.windows)-1] {
			if windowTimestamp == w.endTimestamp {
				w.delta += delta
			}
		}
	}
}

func (as *windowedTotalAggrState) pushSamples(samples []pushSample) {
	currentTime := fasttime.UnixTimestamp()
	deleteDeadline := currentTime + as.stalenessSecs
	keepFirstSample := as.keepFirstSample && currentTime > as.ignoreFirstSampleDeadline
	for i := range samples {
		s := &samples[i]
		inputKey, outputKey := getInputOutputKey(s.key)

	again:
		v, ok := as.m.Load(outputKey)
		if !ok {
			// The entry is missing in the map. Try creating it.
			v = &windowedTotalStateValue{
				lastValues: make(map[string]windowedLastValueState),
			}
			vNew, loaded := as.m.LoadOrStore(outputKey, v)
			if loaded {
				// Use the entry created by a concurrent goroutine.
				v = vNew
			}
		}
		sv := v.(*windowedTotalStateValue)
		sv.mu.Lock()
		deleted := sv.deleted
		if !deleted {
			lv, ok := sv.lastValues[inputKey]
			outOfOrder := ok && lv.timestamp > s.timestamp
			if !as.ignoreOutOfOrderSamples || !outOfOrder {
				if ok || keepFirstSample {
					if s.value >= lv.value {
						as.pushDelta(sv, s.value-lv.value, s.timestamp)
					} else {
						// counter reset
						as.pushDelta(sv, s.value, s.timestamp)
					}
				}
				lv.value = s.value
				lv.timestamp = s.timestamp
				lv.deleteDeadline = deleteDeadline
				sv.lastValues[inputKey] = lv
				sv.deleteDeadline = deleteDeadline
			}
		}
		sv.mu.Unlock()
		if deleted {
			// The entry has been deleted by the concurrent call to flushState
			// Try obtaining and updating the entry again.
			goto again
		}
	}
}

func (as *windowedTotalAggrState) removeOldEntries(currentTime uint64) {
	m := &as.m
	m.Range(func(k, v interface{}) bool {
		sv := v.(*windowedTotalStateValue)

		sv.mu.Lock()
		deleted := currentTime > sv.deleteDeadline
		if deleted {
			// Mark the current entry as deleted
			sv.deleted = deleted
		} else {
			// Delete outdated entries in sv.lastValues
			m := sv.lastValues
			for k1, v1 := range m {
				if currentTime > v1.deleteDeadline {
					delete(m, k1)
				}
			}
		}
		sv.mu.Unlock()

		if deleted {
			m.Delete(k)
		}
		return true
	})
}

func (as *windowedTotalAggrState) flushState(ctx *flushCtx, resetState bool) {
	currentTime := fasttime.UnixTimestamp()

	as.removeOldEntries(currentTime)

	m := &as.m
	m.Range(func(k, v interface{}) bool {
		sv := v.(*windowedTotalStateValue)
		sv.mu.Lock()
		windowsToFlush := sv.windows[:len(sv.windows)-1]
		for _, w := range windowsToFlush {
			w.delta = sv.total + w.delta // hack
		}
		sv.windows = sv.windows[len(sv.windows)-1:]

		if resetState {
			if as.resetTotalOnFlush {
				sv.total = 0
			} else if math.Abs(sv.total) >= (1 << 53) {
				// It is time to reset the entry, since it starts losing float64 precision
				sv.total = 0
			}
		}
		deleted := sv.deleted
		sv.mu.Unlock()
		if !deleted {
			key := k.(string)
			for _, w := range windowsToFlush {
				ctx.appendSeries(key, as.suffix, w.endTimestamp, w.delta)
			}
		}
		return true
	})
}
