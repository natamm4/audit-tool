package filter

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	auditv1 "k8s.io/apiserver/pkg/apis/audit/v1"
)

type AuditFilter interface {
	FilterEvents(events ...*auditv1.Event) []*auditv1.Event
}

type AuditFilters []AuditFilter

func (f AuditFilters) FilterEvents(events ...*auditv1.Event) []*auditv1.Event {
	ret := make([]*auditv1.Event, len(events))
	copy(ret, events)

	for _, filter := range f {
		ret = filter.FilterEvents(ret...)
	}

	return ret
}

type FilterByFailures struct {
}

func (f *FilterByFailures) FilterEvents(events ...*auditv1.Event) []*auditv1.Event {
	ret := []*auditv1.Event{}
	for i := range events {
		event := events[i]
		if event.ResponseStatus == nil {
			continue
		}
		if event.ResponseStatus.Code > 299 {
			ret = append(ret, event)
		}
	}

	return ret
}

type FilterByHTTPStatus struct {
	HTTPStatusCodes sets.Int32
}

func (f *FilterByHTTPStatus) FilterEvents(events ...*auditv1.Event) []*auditv1.Event {
	ret := []*auditv1.Event{}
	for i := range events {
		event := events[i]
		if event.ResponseStatus == nil {
			continue
		}
		if f.HTTPStatusCodes.Has(event.ResponseStatus.Code) {
			ret = append(ret, event)
		}
	}

	return ret
}

type FilterByNamespaces struct {
	Namespaces sets.String
}

func (f *FilterByNamespaces) FilterEvents(events ...*auditv1.Event) []*auditv1.Event {
	ret := []*auditv1.Event{}
	for i := range events {
		event := events[i]
		ns, _, _, _ := URIToParts(event.RequestURI)

		if AcceptString(f.Namespaces, ns) {
			ret = append(ret, event)
		}
	}

	return ret
}

type FilterBySubresources struct {
	Subresources sets.String
}

func (f *FilterBySubresources) FilterEvents(events ...*auditv1.Event) []*auditv1.Event {
	ret := []*auditv1.Event{}
	for i := range events {
		event := events[i]
		_, _, _, subresource := URIToParts(event.RequestURI)

		if f.Subresources.Has("-*") && len(f.Subresources) == 1 && len(subresource) == 0 {
			ret = append(ret, event)
			continue
		}
		if AcceptString(f.Subresources, subresource) {
			ret = append(ret, event)
		}
	}

	return ret
}

type FilterByNames struct {
	Names sets.String
}

func (f *FilterByNames) FilterEvents(events ...*auditv1.Event) []*auditv1.Event {
	ret := []*auditv1.Event{}
	for i := range events {
		event := events[i]
		_, _, name, _ := URIToParts(event.RequestURI)

		if AcceptString(f.Names, name) {
			ret = append(ret, event)
			continue
		}

		// if we didn't match, check the objectref
		if event.ObjectRef == nil {
			continue
		}

		if AcceptString(f.Names, event.ObjectRef.Name) {
			ret = append(ret, event)
		}
	}

	return ret
}

type FilterByUIDs struct {
	UIDs sets.String
}

func (f *FilterByUIDs) FilterEvents(events ...*auditv1.Event) []*auditv1.Event {
	ret := []*auditv1.Event{}
	for i := range events {
		event := events[i]

		if AcceptString(f.UIDs, string(event.AuditID)) {
			ret = append(ret, event)
		}
	}

	return ret
}

type FilterByUser struct {
	Users sets.String
}

func (f *FilterByUser) FilterEvents(events ...*auditv1.Event) []*auditv1.Event {
	ret := []*auditv1.Event{}
	for i := range events {
		event := events[i]

		if AcceptString(f.Users, event.User.Username) {
			ret = append(ret, event)
		}
	}

	return ret
}

type FilterByVerbs struct {
	Verbs sets.String
}

func (f *FilterByVerbs) FilterEvents(events ...*auditv1.Event) []*auditv1.Event {
	ret := []*auditv1.Event{}
	for i := range events {
		event := events[i]

		if AcceptString(f.Verbs, event.Verb) {
			ret = append(ret, event)
		}
	}

	return ret
}

type FilterByResources struct {
	Resources map[schema.GroupResource]bool
}

func (f *FilterByResources) FilterEvents(events ...*auditv1.Event) []*auditv1.Event {
	ret := []*auditv1.Event{}
	for i := range events {
		event := events[i]
		_, gvr, _, _ := URIToParts(event.RequestURI)
		antiMatch := schema.GroupResource{Resource: "-" + gvr.Resource, Group: gvr.Group}

		// check for an anti-match
		if f.Resources[antiMatch] {
			continue
		}
		if f.Resources[gvr.GroupResource()] {
			ret = append(ret, event)
		}

		// if we aren't an exact match, match on resource only if group is '*'
		// check for an anti-match
		antiMatched := false
		for currResource := range f.Resources {
			if currResource.Group == "*" && currResource.Resource == antiMatch.Resource {
				antiMatched = true
				break
			}
			if currResource.Resource == "-*" && currResource.Group == gvr.Group {
				antiMatched = true
				break
			}
		}
		if antiMatched {
			continue
		}

		for currResource := range f.Resources {
			if currResource.Group == "*" && currResource.Resource == "*" {
				ret = append(ret, event)
				break
			}
			if currResource.Group == "*" && currResource.Resource == gvr.Resource {
				ret = append(ret, event)
				break
			}
			if currResource.Resource == "*" && currResource.Group == gvr.Group {
				ret = append(ret, event)
				break
			}
		}
	}

	return ret
}

func URIToParts(uri string) (string, schema.GroupVersionResource, string, string) {
	ns := ""
	gvr := schema.GroupVersionResource{}
	name := ""

	if len(uri) >= 1 {
		if uri[0] == '/' {
			uri = uri[1:]
		}
	}

	// some request URL has query parameters like: /apis/image.openshift.io/v1/images?limit=500&resourceVersion=0
	// we are not interested in the query parameters.
	uri = strings.Split(uri, "?")[0]
	parts := strings.Split(uri, "/")
	if len(parts) == 0 {
		return ns, gvr, name, ""
	}
	// /api/v1/namespaces/<name>
	if parts[0] == "api" {
		if len(parts) >= 2 {
			gvr.Version = parts[1]
		}
		if len(parts) < 3 {
			return ns, gvr, name, ""
		}

		switch {
		case parts[2] != "namespaces": // cluster scoped request that is not a namespace
			gvr.Resource = parts[2]
			if len(parts) >= 4 {
				name = parts[3]
				return ns, gvr, name, ""
			}
		case len(parts) == 3 && parts[2] == "namespaces": // a namespace request /api/v1/namespaces
			gvr.Resource = parts[2]
			return "", gvr, "", ""

		case len(parts) == 4 && parts[2] == "namespaces": // a namespace request /api/v1/namespaces/<name>
			gvr.Resource = parts[2]
			name = parts[3]
			ns = parts[3]
			return ns, gvr, name, ""

		case len(parts) == 5 && parts[2] == "namespaces" && parts[4] == "finalize", // a namespace request /api/v1/namespaces/<name>/finalize
			len(parts) == 5 && parts[2] == "namespaces" && parts[4] == "status": // a namespace request /api/v1/namespaces/<name>/status
			gvr.Resource = parts[2]
			name = parts[3]
			ns = parts[3]
			return ns, gvr, name, parts[4]

		default:
			// this is not a cluster scoped request and not a namespace request we recognize
		}

		if len(parts) < 4 {
			return ns, gvr, name, ""
		}

		ns = parts[3]
		if len(parts) >= 5 {
			gvr.Resource = parts[4]
		}
		if len(parts) >= 6 {
			name = parts[5]
		}
		if len(parts) >= 7 {
			return ns, gvr, name, strings.Join(parts[6:], "/")
		}
		return ns, gvr, name, ""
	}

	if parts[0] != "apis" {
		return ns, gvr, name, ""
	}

	// /apis/group/v1/namespaces/<name>
	if len(parts) >= 2 {
		gvr.Group = parts[1]
	}
	if len(parts) >= 3 {
		gvr.Version = parts[2]
	}
	if len(parts) < 4 {
		return ns, gvr, name, ""
	}

	if parts[3] != "namespaces" {
		gvr.Resource = parts[3]
		if len(parts) >= 5 {
			name = parts[4]
			return ns, gvr, name, ""
		}
	}
	if len(parts) < 5 {
		return ns, gvr, name, ""
	}

	ns = parts[4]
	if len(parts) >= 6 {
		gvr.Resource = parts[5]
	}
	if len(parts) >= 7 {
		name = parts[6]
	}
	if len(parts) >= 8 {
		return ns, gvr, name, strings.Join(parts[7:], "/")
	}
	return ns, gvr, name, ""
}

type FilterByStage struct {
	Stages sets.String
}

func (f *FilterByStage) FilterEvents(events ...*auditv1.Event) []*auditv1.Event {
	// in case we end up calling the filter with an empty set of stage then we have nothing to filter.
	if len(f.Stages) == 0 {
		return events
	}

	ret := []*auditv1.Event{}
	for i := range events {
		event := events[i]

		// TODO: an event not having a stage, what do we do?
		if f.Stages.Has(string(event.Stage)) {
			ret = append(ret, event)
		}
	}

	return ret
}

type FilterByAfter struct {
	After time.Time
}

func (f *FilterByAfter) FilterEvents(events ...*auditv1.Event) []*auditv1.Event {
	ret := []*auditv1.Event{}
	for i := range events {
		event := events[i]
		if event.RequestReceivedTimestamp.After(f.After) {
			ret = append(ret, event)
		}
	}

	return ret
}

type FilterByBefore struct {
	Before time.Time
}

func (f *FilterByBefore) FilterEvents(events ...*auditv1.Event) []*auditv1.Event {
	t := metav1.NewMicroTime(f.Before)
	ret := []*auditv1.Event{}
	for i := range events {
		event := events[i]
		if event.RequestReceivedTimestamp.Before(&t) {
			ret = append(ret, event)
		}
	}

	return ret
}

type FilterByDuration struct {
	Duration time.Duration
}

func (f *FilterByDuration) FilterEvents(events ...*auditv1.Event) []*auditv1.Event {
	ret := []*auditv1.Event{}
	for i := range events {
		event := events[i]
		if event.StageTimestamp.Sub(event.RequestReceivedTimestamp.Time) <= f.Duration {
			ret = append(ret, event)
		}
	}

	return ret
}

func AcceptString(allowedValues sets.String, currValue string) bool {
	// check for an anti-match
	if allowedValues.Has("-" + currValue) {
		return false
	}
	for _, allowedValue := range allowedValues.UnsortedList() {
		if !strings.HasSuffix(allowedValue, "*") || !strings.HasPrefix(allowedValue, "-") {
			continue
		}
		if strings.HasPrefix("-"+currValue, allowedValue[:len(allowedValue)-1]) {
			return false
		}
	}

	// if all values are negation, assume * by default
	allValuesNegative := true
	for _, allowedValue := range allowedValues.UnsortedList() {
		if !strings.HasPrefix(allowedValue, "-") {
			allValuesNegative = false
			break
		}
	}
	if allValuesNegative {
		return true
	}

	if allowedValues.Has(currValue) {
		return true
	}
	for _, allowedValue := range allowedValues.UnsortedList() {
		if !strings.HasSuffix(allowedValue, "*") || strings.HasPrefix(allowedValue, "-") {
			continue
		}
		if strings.HasPrefix(currValue, allowedValue[:len(allowedValue)-1]) {
			return true
		}
	}

	return false
}

type FilterByAround struct {
	Around         string
	AroundDuration time.Duration
}

func (f *FilterByAround) FilterEvents(events ...*corev1.Event) []*corev1.Event {
	t := events[len(events)-1].LastTimestamp.Time
	aroundParts := strings.Split(f.Around, ":")
	if len(aroundParts) < 2 || len(aroundParts) > 3 {
		fmt.Fprintf(os.Stderr, "invalid around time format, must be HH:MM or HH:MM:SS, got %q", f.Around)
		return nil
	}
	aroundTimeHours, err := strconv.Atoi(aroundParts[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "error parsing around time: %b", err)
		return nil
	}
	aroundTimeMinutes, err := strconv.Atoi(aroundParts[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "error parsing around time: %b", err)
		return nil
	}
	aroundTimeSeconds := 0
	if len(aroundParts) > 2 {
		aroundTimeSeconds, err = strconv.Atoi(aroundParts[2])
		if err != nil {
			fmt.Fprintf(os.Stderr, "error parsing around time: %b", err)
			return nil
		}
	}

	aroundTime := time.Date(t.Year(), t.Month(), t.Day(), aroundTimeHours, aroundTimeMinutes, aroundTimeSeconds, t.Nanosecond(), t.Location())
	ret := []*corev1.Event{}
	for i := range events {
		event := events[i]
		if event.LastTimestamp.Time.After(aroundTime.Add(f.AroundDuration)) || event.LastTimestamp.Time.Before(aroundTime.Add(-f.AroundDuration)) {
			continue
		}
		ret = append(ret, event)
	}

	return ret
}
