package streamaggr

import (
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"math"
	"slices"
	"sync"
	"time"
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

	// The maximum time a sample can be delayed in seconds.
	maxDelayedSampleSecs uint64

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

	// Used for testing.
	getUnixTimestamp func() uint64
}

type windowedTotalStateValue struct {
	mu             sync.Mutex
	lastValues     map[string]windowedLastValueState
	windows        map[uint64]float64
	total          float64
	deleteDeadline uint64
	deleted        bool
}

type windowedLastValueState struct {
	value          float64
	timestamp      int64
	deleteDeadline uint64
}

func newWindowedTotalAggrState(interval time.Duration, stalenessInterval time.Duration, resetTotalOnFlush, keepFirstSample bool, ignoreOutOfOrderSamples bool, getUnixTimestamp func() uint64) *windowedTotalAggrState {
	stalenessSecs := roundDurationToSecs(stalenessInterval)
	intervalSecs := roundDurationToSecs(interval)
	ignoreFirstSampleDeadline := getUnixTimestamp() + stalenessSecs
	suffix := "total"
	if resetTotalOnFlush {
		suffix = "increase"
	}
	return &windowedTotalAggrState{
		suffix:                    suffix,
		resetTotalOnFlush:         resetTotalOnFlush,
		keepFirstSample:           keepFirstSample,
		intervalSecs:              intervalSecs,
		maxDelayedSampleSecs:      intervalSecs, // TODO: parameterize
		stalenessSecs:             stalenessSecs,
		ignoreFirstSampleDeadline: ignoreFirstSampleDeadline,
		ignoreOutOfOrderSamples:   ignoreOutOfOrderSamples,
		getUnixTimestamp:          getUnixTimestamp,
	}
}

// roundUp rounds up n to the nearest r.
func roundUp(n, r uint64) uint64 {
	if n%r == 0 {
		return n
	}
	return r - (n % r) + n
}

func (as *windowedTotalAggrState) pushSample(sv *windowedTotalStateValue, delta float64, timestamp uint64) {
	key := roundUp(timestamp, as.intervalSecs)
	if _, ok := sv.windows[key]; !ok {
		sv.windows[key] = 0
	}
	sv.windows[key] += delta
}

func (as *windowedTotalAggrState) pushSamples(samples []pushSample) {
	currentTime := as.getUnixTimestamp()
	tooLateDeadline := currentTime - as.maxDelayedSampleSecs
	deleteDeadline := currentTime + as.stalenessSecs
	keepFirstSample := as.keepFirstSample && currentTime > as.ignoreFirstSampleDeadline

	for i := range samples {
		s := &samples[i]
		timestampSecs := uint64(s.timestamp / 1000)
		if timestampSecs < tooLateDeadline {
			//	logger.Infof("[windowed_total]: sample too late\n")
			continue
		}
		if timestampSecs > currentTime+5 {
			logger.Infof("[windowed_total]: sample too far far in future: %d (vs. %d)\n", timestampSecs, currentTime)
			//	continue
		}

		inputKey, outputKey := getInputOutputKey(s.key)

	again:
		v, ok := as.m.Load(outputKey)
		if !ok {
			// The entry is missing in the map. Try creating it.
			v = &windowedTotalStateValue{
				lastValues: make(map[string]windowedLastValueState),
				windows:    make(map[uint64]float64),
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
						as.pushSample(sv, s.value-lv.value, timestampSecs)
					} else {
						// counter reset
						as.pushSample(sv, s.value, timestampSecs)
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
	currentTime := as.getUnixTimestamp()
	tooLateDeadline := currentTime - as.maxDelayedSampleSecs

	as.removeOldEntries(currentTime)

	m := &as.m
	m.Range(func(k, v interface{}) bool {
		sv := v.(*windowedTotalStateValue)
		sv.mu.Lock()

		var flushTimestamps []uint64 // TODO: optimize
		for timestamp := range sv.windows {
			if timestamp < tooLateDeadline {
				flushTimestamps = append(flushTimestamps, timestamp)
			}
		}
		slices.SortFunc(flushTimestamps, func(a, b uint64) int {
			if a < b {
				return -1
			} else if a > b {
				return 1
			}
			return 0
		})

		var flushTotals []float64
		for _, timestamp := range flushTimestamps {
			sv.total += sv.windows[timestamp]
			flushTotals = append(flushTotals, sv.total)
			delete(sv.windows, timestamp)
		}

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
			for i, timestamp := range flushTimestamps {
				ctx.appendSeries(key, as.suffix, int64(timestamp*1000), flushTotals[i])
			}
		}
		return true
	})
}
