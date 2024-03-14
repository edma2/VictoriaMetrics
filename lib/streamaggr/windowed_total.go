package streamaggr

import (
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/metrics"
	"math"
	"slices"
	"sync"
	"time"
)

// windowedTotalAggrState calculates output=total, e.g. the summary counter over input counters.
type windowedTotalAggrState struct {
	m sync.Map

	suffix string

	// The time interval
	intervalSecs uint64

	// The maximum time a sample can be delayed in seconds.
	maxDelaySecs uint64

	// Time series state is dropped if no new samples are received during stalenessSecs.
	//
	// Aslo, the first sample per each new series is ignored during stalenessSecs even if keepFirstSample is set.
	// see ignoreFirstSampleDeadline for more details.
	stalenessSecs uint64

	// Used for testing.
	getUnixTimestamp func() uint64

	lateSamples *metrics.Counter
}

type windowedTotalStateValue struct {
	mu             sync.Mutex
	pendingSamples []pendingSample
	lastValues     map[string]windowedLastValueState
	total          float64
	deleteDeadline uint64
	deleted        bool
}

type pendingSample struct {
	key       string
	value     float64
	timestamp int64
}

type windowedLastValueState struct {
	lastFlushedValue float64
	deleteDeadline   uint64
	hasFlushed       bool
}

func newWindowedTotalAggrState(interval, stalenessInterval, maxDelay time.Duration, getUnixTimestamp func() uint64, lateSamples *metrics.Counter) *windowedTotalAggrState {
	stalenessSecs := roundDurationToSecs(stalenessInterval)
	intervalSecs := roundDurationToSecs(interval)
	maxDelaySecs := roundDurationToSecs(maxDelay)
	suffix := "total"
	return &windowedTotalAggrState{
		suffix:           suffix,
		intervalSecs:     intervalSecs,
		maxDelaySecs:     maxDelaySecs,
		stalenessSecs:    stalenessSecs,
		getUnixTimestamp: getUnixTimestamp,
		lateSamples:      lateSamples,
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
	tooLateDeadline := currentTime - as.maxDelaySecs
	deleteDeadline := currentTime + as.stalenessSecs

	for i := range samples {
		s := &samples[i]
		timestampSecs := uint64(s.timestamp / 1000)
		if timestampSecs < tooLateDeadline {
			as.lateSamples.Inc()
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
			sv.pendingSamples = append(sv.pendingSamples, pendingSample{inputKey, s.value, s.timestamp})
			lv, _ := sv.lastValues[inputKey]
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

func compareByTimestamp(a, b pendingSample) int {
	if a.timestamp < b.timestamp {
		return -1
	} else if a.timestamp > b.timestamp {
		return 1
	}
	return 0
}

func (as *windowedTotalAggrState) flushState(ctx *flushCtx, resetState bool) {
	currentTime := as.getUnixTimestamp()
	flushDeadline := currentTime - as.maxDelaySecs

	as.removeOldEntries(currentTime)

	m := &as.m
	m.Range(func(k, v interface{}) bool {
		sv := v.(*windowedTotalStateValue)
		sv.mu.Lock()

		slices.SortFunc(sv.pendingSamples, compareByTimestamp)
		windows := make(map[uint64]float64)
		var windowsToFlush []uint64
		i := 0
		for _, s := range sv.pendingSamples {
			timestampSecs := uint64(s.timestamp / 1000)
			windowKey := roundUp(timestampSecs, as.intervalSecs)
			// the sample's window is not ready to be flushed
			if windowKey > flushDeadline {
				break
			}
			lv, ok := sv.lastValues[s.key]
			if ok {
				if lv.hasFlushed {
					delta := s.value
					if s.value >= lv.lastFlushedValue {
						delta = s.value - lv.lastFlushedValue
					}
					if _, ok := windows[windowKey]; !ok {
						windowsToFlush = append(windowsToFlush, windowKey)
					}
					windows[windowKey] += delta
				}
				lv.lastFlushedValue = s.value
				lv.hasFlushed = true
				sv.lastValues[s.key] = lv
			}
			i++
		}
		sv.pendingSamples = sv.pendingSamples[i:]

		if resetState {
			if math.Abs(sv.total) >= (1 << 53) {
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
