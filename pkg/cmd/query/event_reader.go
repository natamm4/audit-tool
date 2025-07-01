package query

import (
	"bufio"
	"compress/gzip"
	"log"
	"os"
	"sort"

	"github.com/natamm4/audit-tool/pkg/audit/filter"

	jsoniter "github.com/json-iterator/go"
	auditv1 "k8s.io/apiserver/pkg/apis/audit/v1"
)

func decodeAuditEvents(name string, filters ...filter.AuditFilters) ([]*auditv1.Event, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	gzipReader, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gzipReader.Close()

	fileScanner := bufio.NewScanner(gzipReader)
	fileScanner.Split(bufio.ScanLines)
	events := []*auditv1.Event{}

	for fileScanner.Scan() {
		event := auditv1.Event{}
		eventBytes := fileScanner.Bytes()
		if err := jsoniter.Unmarshal(eventBytes, &event); err != nil {
			log.Printf("failed to unmarshal audit event: %q: %v", string(eventBytes), err)
		}
		events = append(events, &event)
	}

	for _, f := range filters {
		events = f.FilterEvents(events...)
	}
	sort.Slice(events, func(i, j int) bool {
		return events[i].RequestReceivedTimestamp.After(events[i].RequestReceivedTimestamp.Time)
	})

	return events, nil
}
