package query

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/util/sets"
	auditv1 "k8s.io/apiserver/pkg/apis/audit/v1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"

	"github.com/natamm4/audit-tool/pkg/audit/filter"
)

type Options struct {
	targetDirectory string
	nodes           []string
	from, to        string
	limit           int64

	nodeNames  sets.String
	auditFiles *AuditDirReader

	verbs           []string
	resources       []string
	subresources    []string
	namespaces      []string
	names           []string
	users           []string
	uids            []string
	filenames       []string
	failedOnly      bool
	httpStatusCodes []int32
	output          string
	topBy           string
	stages          []string
	duration        string

	stats bool
}

func NewCommand(ctx context.Context, f cmdutil.Factory, streams genericclioptions.IOStreams) *cobra.Command {
	options := &Options{}
	cmd := &cobra.Command{
		Use:   "query",
		Short: "Run queries against downloaded audit log files",
		Run: func(cmd *cobra.Command, args []string) {
			cmdutil.CheckErr(options.Validate())
			cmdutil.CheckErr(options.Complete())
			cmdutil.CheckErr(options.Run(ctx))
		},
	}

	cmd.Flags().StringVarP(&options.targetDirectory, "dir", "d", "", "Directory to read the audit files from")
	cmd.Flags().StringSliceVar(&options.nodes, "nodes", []string{}, "Specify nodes to query audit events. Empty means all nodes.")
	cmd.Flags().BoolVarP(&options.stats, "stats", "", false, "Display stats from provided directory (start/end times, nodes, etc.")
	cmd.Flags().Int64VarP(&options.limit, "limit", "", 0, "Limit the amount of events to display")

	cmd.Flags().StringVar(&options.from, "from", "", "Only query events starting at this time (eg: '2006-01-02 15:03:04')")
	cmd.Flags().StringVar(&options.to, "to", "", "Only query events before this time (eg: '2006-01-02 15:03:04')")

	cmd.Flags().StringSliceVar(&options.uids, "uid", options.uids, "Only match specific UIDs")
	cmd.Flags().StringSliceVar(&options.verbs, "verb", options.verbs, "Filter result of search to only contain the specified verb (eg. 'update', 'get', etc..)")
	cmd.Flags().StringSliceVar(&options.resources, "resource", options.resources, "Filter result of search to only contain the specified resource.)")
	cmd.Flags().StringSliceVar(&options.subresources, "subresource", options.subresources, "Filter result of search to only contain the specified subresources.  \"-*\" means no subresource)")
	cmd.Flags().StringSliceVarP(&options.namespaces, "namespace", "n", options.namespaces, "Filter result of search to only contain the specified namespace.")
	cmd.Flags().StringSliceVar(&options.names, "name", options.names, "Filter result of search to only contain the specified name.)")
	cmd.Flags().StringSliceVar(&options.users, "user", options.users, "Filter result of search to only contain the specified user.)")
	cmd.Flags().StringVar(&options.topBy, "by", options.topBy, "Switch the top output format (eg. -o top -by [verb,user,resource,httpstatus,namespace]).")
	cmd.Flags().StringVarP(&options.output, "output", "o", options.output, "Specify the output format (e.g. 'openmetrics', 'default')")
	cmd.Flags().BoolVar(&options.failedOnly, "failed-only", false, "Filter result of search to only contain http failures.")
	cmd.Flags().Int32SliceVar(&options.httpStatusCodes, "http-status-code", options.httpStatusCodes, "Filter result of search to only certain http status codes (200,429).")
	cmd.Flags().StringSliceVarP(&options.stages, "stage", "s", options.stages, "Filter result by event stage (eg. 'RequestReceived', 'ResponseComplete'), if omitted all stages will be included)")
	cmd.Flags().StringVar(&options.duration, "duration", options.duration, "Filter all requests that didn't take longer than the specified timeout to complete. Keep in mind that requests usually don't take exactly the specified time. Adding a second or two should give you what you want.")
	return cmd
}

func (o Options) Validate() error {
	if len(o.targetDirectory) == 0 {
		return fmt.Errorf("directory with audit files must be specified (--dir/-d)")
	}
	return nil
}

func (o *Options) Complete() error {
	files, err := NewAuditDirReader(o.targetDirectory)
	if err != nil {
		return err
	}
	o.nodeNames = sets.NewString()
	for n := range files.files {
		o.nodeNames.Insert(n)
	}
	requestNodes := sets.NewString(o.nodes...)
	if len(o.nodes) > 0 && !o.nodeNames.HasAll(requestNodes.List()...) {
		return fmt.Errorf("invalid nodes: %s, valid node names are: %s", strings.Join(requestNodes.List(), ","), strings.Join(o.nodeNames.List(), ","))
	}
	o.auditFiles = files
	return nil
}

const timeDefaultFormat = "2006-01-02 15:04:05"

func parseTime(s string) time.Time {
	t, err := time.Parse(timeDefaultFormat, s)
	if err != nil {
		log.Fatal("invalid time format: %q, use %q", s, timeDefaultFormat)
	}
	return t
}

func (o Options) runStats() error {
	nodes := []string{}
	for nodeName := range o.auditFiles.files {
		nodes = append(nodes, nodeName)
	}

	list := []pterm.BulletListItem{}
	for _, n := range nodes {
		list = append(list, pterm.BulletListItem{
			Level: 0,
			Text:  fmt.Sprintf("%s", pterm.NewStyle(pterm.BgBlack, pterm.FgLightWhite).Sprintf(n)),
		})
		list = append(list, pterm.BulletListItem{
			Level: 1,
			Text:  fmt.Sprintf("from: %s | to: %s", printTime(o.auditFiles.files[n][len(o.auditFiles.files[n])-1].timestamp), printTime(o.auditFiles.files[n][0].timestamp)),
		})
	}

	err := pterm.DefaultBulletList.WithItems(list).Render()
	if err != nil {
		return err
	}
	return nil
}

func isInTimeRange(from, to string, timestamp time.Time) bool {
	var fromTime, toTime time.Time
	fromTime = time.Now().Add(-365 * 24 * time.Hour) // one year is default
	if len(from) != 0 {
		fromTime = parseTime(from)
	}
	toTime = time.Now()
	if len(to) != 0 {
		toTime = parseTime(to)
	}
	return timestamp.After(fromTime) && timestamp.Before(toTime)
}

func (o Options) multiNodeEventDecoder(filters filter.AuditFilters) ([]*auditv1.Event, error) {
	requestNodes := sets.NewString(o.nodes...)
	result := []*auditv1.Event{}
	processedFiles := 0
	for _, n := range o.nodeNames.List() {
		if requestNodes.Len() > 0 && !requestNodes.Has(n) {
			continue
		}
		for _, nodeAuditFile := range o.auditFiles.files[n] {
			if !isInTimeRange(o.from, o.to, nodeAuditFile.timestamp) {
				continue
			}
			//log.Printf("decoding %q (%s) ...", nodeAuditFile.name, nodeAuditFile.timestamp)
			events, err := decodeAuditEvents(nodeAuditFile.filePath, filters)
			if err != nil {
				return nil, fmt.Errorf("reading audit file %q failed: %v", nodeAuditFile.name, err)
			}
			processedFiles++
			result = append(result, events...)
		}
	}
	//log.Printf("processed %d audit files", processedFiles)
	return result, nil
}

func (o Options) setupFilters() (filter.AuditFilters, error) {
	filters := filter.AuditFilters{}
	if len(o.uids) > 0 {
		filters = append(filters, &filter.FilterByUIDs{UIDs: sets.NewString(o.uids...)})
	}
	if len(o.names) > 0 {
		filters = append(filters, &filter.FilterByNames{Names: sets.NewString(o.names...)})
	}
	if len(o.namespaces) > 0 {
		filters = append(filters, &filter.FilterByNamespaces{Namespaces: sets.NewString(o.namespaces...)})
	}
	if len(o.stages) > 0 {
		filters = append(filters, &filter.FilterByStage{Stages: sets.NewString(o.stages...)})
	}
	if len(o.to) > 0 {
		t, err := time.Parse(timeDefaultFormat, o.to)
		if err != nil {
			return nil, err
		}
		filters = append(filters, &filter.FilterByBefore{Before: t})
	}
	if len(o.from) > 0 {
		t, err := time.Parse(timeDefaultFormat, o.from)
		if err != nil {
			return nil, err
		}
		filters = append(filters, &filter.FilterByAfter{After: t})
	}
	if len(o.resources) > 0 {
		resources := map[schema.GroupResource]bool{}
		for _, resource := range o.resources {
			parts := strings.Split(resource, ".")
			gr := schema.GroupResource{}
			gr.Resource = parts[0]
			if len(parts) >= 2 {
				gr.Group = strings.Join(parts[1:], ".")
			}
			resources[gr] = true
		}

		filters = append(filters, &filter.FilterByResources{Resources: resources})
	}
	if len(o.subresources) > 0 {
		filters = append(filters, &filter.FilterBySubresources{Subresources: sets.NewString(o.subresources...)})
	}
	if len(o.users) > 0 {
		filters = append(filters, &filter.FilterByUser{Users: sets.NewString(o.users...)})
	}
	if len(o.verbs) > 0 {
		filters = append(filters, &filter.FilterByVerbs{Verbs: sets.NewString(o.verbs...)})
	}
	if len(o.httpStatusCodes) > 0 {
		filters = append(filters, &filter.FilterByHTTPStatus{HTTPStatusCodes: sets.NewInt32(o.httpStatusCodes...)})
	}
	if o.failedOnly {
		filters = append(filters, &filter.FilterByFailures{})
	}
	if len(o.duration) > 0 {
		d, err := time.ParseDuration(o.duration)
		if err != nil {
			return nil, err
		}
		filters = append(filters, &filter.FilterByDuration{d})
	}

	return filters, nil
}

func (o Options) Run(ctx context.Context) error {
	if o.stats {
		return o.runStats()
	}

	filters, err := o.setupFilters()
	if err != nil {
		return err
	}

	events, err := o.multiNodeEventDecoder(filters)
	if err != nil {
		return err
	}

	switch o.output {
	case "openmetrics":
		return PrintOpenMetrics(events, os.Stdout)
	default:
		for i, e := range events {
			if o.limit > 0 && i > int(o.limit) {
				break
			}
			pterm.Println(printEvent(e))
		}
	}
	return nil
}
