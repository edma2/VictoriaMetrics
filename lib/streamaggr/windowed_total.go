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
	pendingSamples []pushSample
	lastValues     map[string]windowedLastValueState
	total          float64
	deleteDeadline uint64
	deleted        bool
}

type windowedLastValueState struct {
	value          float64
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

func (as *windowedTotalAggrState) pushSamples(samples []pushSample) {
	currentTime := as.getUnixTimestamp()
	tooLateDeadline := currentTime - as.maxDelayedSampleSecs
	deleteDeadline := currentTime + as.stalenessSecs

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
			if ok {
				// We saw this sample before so we can compute a delta.
				sv.pendingSamples = append(sv.pendingSamples, *s)
			} else {
				// if it's our first time seeing it, don't add a pending sample but initialize the value
				// so the next sample takes the delta.
				lv.value = s.value
			}
			lv.deleteDeadline = deleteDeadline
			sv.lastValues[inputKey] = lv
			sv.deleteDeadline = deleteDeadline
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

func sortSamplesByTimestamp(samples []pushSample) {
	slices.SortFunc(samples, func(a, b pushSample) int {
		if a.timestamp < b.timestamp {
			return -1
		} else if a.timestamp > b.timestamp {
			return 1
		}
		return 0
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

		logger.Infof("pending samples: %v\n", sv.pendingSamples)

		sortSamplesByTimestamp(sv.pendingSamples)
		windows := make(map[uint64]float64)
		var windowsToFlush []uint64
		i := 0
		for _, s := range sv.pendingSamples {
			timestampSecs := uint64(s.timestamp / 1000)
			windowKey := roundUp(timestampSecs, as.intervalSecs)
			// the sample's window is not ready to be flushed
			if windowKey > tooLateDeadline {
				break
			}
			inputKey, _ := getInputOutputKey(s.key)
			lv, ok := sv.lastValues[inputKey]
			if ok {
				delta := s.value
				if s.value >= lv.value {
					delta = s.value - lv.value
				}
				if _, ok := windows[windowKey]; !ok {
					windowsToFlush = append(windowsToFlush, windowKey)
				}
				windows[windowKey] += delta
				lv.value = s.value
				sv.lastValues[inputKey] = lv
			}
			i++
		}
		logger.Infof("windowsToFlush: %v (deadline: %v)\n", windowsToFlush, tooLateDeadline)
		sv.pendingSamples = sv.pendingSamples[i:]

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
			for _, windowKey := range windowsToFlush {
				windowDelta := windows[windowKey]
				sv.total += windowDelta
				ctx.appendSeries(key, as.suffix, int64(windowKey*1000), sv.total)
			}
		}
		return true
	})
}
