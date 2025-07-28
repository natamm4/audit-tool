package visualize

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"

	auditv1 "k8s.io/apiserver/pkg/apis/audit/v1"
)

func printOpenmetrics(events []*auditv1.Event, w io.Writer) error {
	fmt.Fprintln(w, "# TYPE audit_event_duration_seconds histogram")

	// define buckets (sec)
	buckets := []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10} // plus +Inf automatically

	type seriesKey struct {
		user, verb, resource, subresource, name, namespace, stage string
		code                                                      int32
		timestamp                                                 int64
	}
	// per-series storage
	type hist struct {
		buckets []uint64
		sum     float64
		count   uint64
	}

	data := map[seriesKey]*hist{}

	// agg every event into buckets
	for _, e := range events {
		k := seriesKey{
			user:      e.User.Username,
			verb:      e.Verb,
			stage:     string(e.Stage),
			code:      e.ResponseStatus.Code,
			timestamp: e.RequestReceivedTimestamp.Time.Unix(),
		}
		if ref := e.ObjectRef; ref != nil {
			k.resource = ref.Resource
			k.subresource = ref.Subresource
			k.name = ref.Name
			k.namespace = ref.Namespace
		}

		h := data[k]
		if h == nil {
			h = &hist{buckets: make([]uint64, len(buckets)+1)} // +1 for +Inf
			data[k] = h
		}
		duration := e.StageTimestamp.Sub(e.RequestReceivedTimestamp.Time).Seconds()
		h.sum += duration
		h.count++

		// find first bucket >= duration
		idx := len(buckets) // default = +Inf
		for i, b := range buckets {
			if duration <= b {
				idx = i
				break
			}
		}
		h.buckets[idx]++
	}

	// sort: labels → timestamp
	keys := make([]seriesKey, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.user != b.user {
			return a.user < b.user
		}
		if a.verb != b.verb {
			return a.verb < b.verb
		}
		if a.resource != b.resource {
			return a.resource < b.resource
		}
		if a.subresource != b.subresource {
			return a.subresource < b.subresource
		}
		if a.name != b.name {
			return a.name < b.name
		}
		if a.namespace != b.namespace {
			return a.namespace < b.namespace
		}
		if a.stage != b.stage {
			return a.stage < b.stage
		}
		if a.code != b.code {
			return a.code < b.code
		}
		return a.timestamp < b.timestamp
	})

	// emit metrics
	for _, k := range keys {
		h := data[k]
		// cumulative count
		cum := uint64(0)
		for i, upper := range append(buckets, math.Inf(1)) {
			cum += h.buckets[i]
			// bucket
			fmt.Fprintf(w, "audit_event_duration_seconds_bucket{user=%q,verb=%q,resource=%q,subresource=%q,name=%q,namespace=%q,stage=%q,code=%q,le=%q} %d %d\n",
				k.user, k.verb, k.resource, k.subresource, k.name, k.namespace, k.stage, strconv.Itoa(int(k.code)), fmt.Sprintf("%g", upper), cum, k.timestamp)
		}
		// sum
		fmt.Fprintf(w, "audit_event_duration_seconds_sum{user=%q,verb=%q,resource=%q,subresource=%q,name=%q,namespace=%q,stage=%q,code=%q} %.6f %d\n",
			k.user, k.verb, k.resource, k.subresource, k.name, k.namespace, k.stage, strconv.Itoa(int(k.code)), h.sum, k.timestamp)
		// count
		fmt.Fprintf(w, "audit_event_duration_seconds_count{user=%q,verb=%q,resource=%q,subresource=%q,name=%q,namespace=%q,stage=%q,code=%q} %d %d\n",
			k.user, k.verb, k.resource, k.subresource, k.name, k.namespace, k.stage, strconv.Itoa(int(k.code)), h.count, k.timestamp)
	}

	fmt.Fprintln(w, "# EOF")
	return nil
}
