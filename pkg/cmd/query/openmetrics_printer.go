package query

import (
	"fmt"
	"io"

	auditv1 "k8s.io/apiserver/pkg/apis/audit/v1"
)

func PrintOpenMetrics(events []*auditv1.Event, w io.Writer) error {
	fmt.Fprintln(w, "# TYPE audit_event_total counter")
	for _, e := range events {
		user := e.User.Username
		verb := e.Verb
		code := int32(0)
		if e.ResponseStatus != nil {
			code = e.ResponseStatus.Code
		}
		fmt.Fprintf(w, "audit_event_total{user=\"%s\",verb=\"%s\",code=\"%d\"} 1\n", user, verb, code)
	}
	return nil
}
