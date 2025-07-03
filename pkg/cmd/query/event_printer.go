package query

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/pterm/pterm"

	auditv1 "k8s.io/apiserver/pkg/apis/audit/v1"
)

func printResponseCode(code int32) string {
	switch {
	case code >= 200 && code < 400:
		return pterm.NewStyle(pterm.FgGreen).Sprintf("%d", code)
	case code >= 400 && code < 500:
		return pterm.NewStyle(pterm.FgLightRed).Sprintf("%d", code)
	case code > 500:
		return pterm.NewStyle(pterm.FgRed).Sprintf("%d", code)
	default:
		return pterm.Sprintf("%d", code)
	}
}

func printRequestURI(u string) string {
	parts := strings.Split(u, "?")
	if len(parts) > 0 {
		return parts[0]
	}
	return u
}

func printUser(e *auditv1.Event) string {
	if len(e.User.Username) > 0 {
		return pterm.NewStyle(pterm.FgGray).Sprintf("%s", strings.ReplaceAll(e.User.Username, "system:serviceaccount:", "sa:"))
	}
	return e.UserAgent
}

func printTime(t time.Time) string {
	return pterm.NewStyle(pterm.FgGray).Sprintf("%s", t.Format(timeDefaultFormat))
}

func printElapsedTime(e *auditv1.Event) string {
	return pterm.NewStyle(pterm.FgWhite).Sprintf("[%s]", e.StageTimestamp.Sub(e.RequestReceivedTimestamp.Time))
}

func printEvent(e *auditv1.Event) string {
	return pterm.Sprintf("[ %s ][ %s ][ %3s ] %s [%s]%s", printTime(e.RequestReceivedTimestamp.Time), pterm.NewStyle(pterm.FgLightWhite).Sprintf("%6s", strings.ToUpper(e.Verb)), printResponseCode(e.ResponseStatus.Code), printRequestURI(e.RequestURI), printUser(e), printElapsedTime(e))
}

func printOpenMetricsCounts(events []*auditv1.Event, w io.Writer) error {
	counts := map[string]int{}

	for _, e := range events {
		user := e.User.Username
		verb := e.Verb
		code := e.ResponseStatus.Code

		key := fmt.Sprintf("%s|%s|%d", user, verb, code)
		counts[key]++
	}

	fmt.Fprintf(w, "# TYPE audit_event_total counter\n")
	for key, count := range counts {
		parts := strings.Split(key, "|")
		user, verb, code := parts[0], parts[1], parts[2]

		fmt.Fprintf(w, "audit_event_total{user=\"%s\",verb=\"%s\",code=\"%s\"} %d\n", user, verb, code, count)
	}
	return nil
}

func printOpenMetricsTimestamps(events []*auditv1.Event, w io.Writer) error {
	fmt.Fprintln(w, "# TYPE audit_event_timestamp gauge")

	for _, e := range events {
		user := e.User.Username
		verb := e.Verb
		code := e.ResponseStatus.Code
		timeStamp := e.RequestReceivedTimestamp.UnixMilli()

		fmt.Fprintf(w, "audit_event_timestamp{user=\"%s\",verb=\"%s\",code=\"%d\"} %d\n", user, verb, code, timeStamp)
	}
	return nil
}
