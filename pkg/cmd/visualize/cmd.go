package visualize

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/util/sets"
	auditv1 "k8s.io/apiserver/pkg/apis/audit/v1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"

	"github.com/natamm4/audit-tool/pkg/audit/filter"
)

type Options struct {
	readDirectory string
	// auditDirectory string
	nodes    []string
	from, to string

	nodeNames  sets.String
	auditFiles *AuditDirReader
}

func NewCommand(f cmdutil.Factory, streams genericclioptions.IOStreams) *cobra.Command {
	options := &Options{}
	cmd := &cobra.Command{
		Use:   "visualize",
		Short: "Create a prometheus instance from downloaded must-gather with audit logs, alerts, and metrics",
		Run: func(cmd *cobra.Command, args []string) {
			cmdutil.CheckErr(options.Validate())
			cmdutil.CheckErr(options.Complete())
			cmdutil.CheckErr(options.Run(cmd.Context()))
		},
	}

	cmd.Flags().StringVarP(&options.readDirectory, "read", "r", "", "Must-Gather directory to read the audit files from.")
	// cmd.Flags().StringVarP(&options.auditDirectory, "audit", "a", "", "Directory to write the audit files to.")

	cmd.Flags().StringVar(&options.from, "from", "", "Only query events starting at this time (eg: '2006-01-02 15:03:04').")
	cmd.Flags().StringVar(&options.to, "to", "", "Only query events before this time (eg: '2006-01-02 15:03:04').")

	return cmd
}

func (o Options) Validate() error {
	if len(o.readDirectory) == 0 {
		return fmt.Errorf("directory with audit files must be specified (--read/-r)")
	}
	return nil
}

func (o *Options) Complete() error {
	auditReader, err := NewAuditDirReader(o.readDirectory)
	if err != nil {
		return err
	}
	// o.nodeNames = sets.NewString()
	// for n := range auditReader.files {
	// 	o.nodeNames.Insert(n)
	// }
	// requestNodes := sets.NewString(o.nodes...)
	// if len(o.nodes) > 0 && !o.nodeNames.HasAll(requestNodes.List()...) {
	// 	return fmt.Errorf("invalid nodes: %s, valid node names are: %s", strings.Join(requestNodes.List(), ","), strings.Join(o.nodeNames.List(), ","))
	// }
	o.auditFiles = auditReader
	return nil
}

const timeDefaultFormat = "2006-01-02 15:04:05"

func parseTime(s string) time.Time {
	t, err := time.Parse(timeDefaultFormat, s)
	if err != nil {
		log.Fatalf("invalid time format: %q, use %q", s, timeDefaultFormat)
	}
	return t
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

func (o Options) Run(ctx context.Context) error {
	// // Get working dir
	// workingDir, err := os.Getwd()
	// if err != nil {
	// 	return err
	// }

	// Use temp dir
	workingDir, err := os.MkdirTemp("", "audit-prometheus-*")
	if err != nil {
		return fmt.Errorf("failed to create temporary directory: %v", err)
	}
	if err := os.Chmod(workingDir, 0755); err != nil {
		return fmt.Errorf("failed to change permissions for %s: %v", workingDir, err)
	}
	defer os.RemoveAll(workingDir)

	workingDir = filepath.Join(workingDir, "prometheus")

	_ = os.MkdirAll(workingDir, 0755)

	// fmt.Println(workingDir)

	// Use finder to locate audit log dir and metrics file
	finderResult, err := FindMustGatherFiles(o.readDirectory)
	if err != nil {
		return err
	}

	// Ensure readDirectory exists
	if err := os.MkdirAll(o.readDirectory, 0755); err != nil {
		return err
	}

	// Read audit events from audit log dir
	auditFilesDir := finderResult.AuditLogDir
	if auditFilesDir == "" {
		return fmt.Errorf("could not find audit_logs/kube-apiserver directory in must-gather")
	}
	auditReader, err := NewAuditDirReader(auditFilesDir)
	if err != nil {
		return err
	}
	o.auditFiles = auditReader
	o.nodeNames = sets.NewString()
	for n := range auditReader.files {
		o.nodeNames.Insert(n)
	}

	// Generate audit.openmetrics
	events, err := o.multiNodeEventDecoder(nil)
	if err != nil {
		return err
	}
	// auditFile := filepath.Join(o.readDirectory, "audit.openmetrics")
	auditFile := filepath.Join(workingDir, "audit.openmetrics")
	auditOut, err := os.Create(auditFile)
	if err != nil {
		return err
	}
	defer auditOut.Close()
	if err := printOpenmetrics(events, auditOut); err != nil {
		return err
	}
	os.Chmod(auditFile, 0644)

	// Use metrics and alerts file paths from finder
	metricsFile := finderResult.MetricsFile
	if metricsFile == "" {
		return fmt.Errorf("could not find etcd_info/metrics.openmetrics in must-gather")
	}
	destMetricsFile := filepath.Join(workingDir, "metrics.openmetrics")
	if contents, err := os.ReadFile(metricsFile); err == nil {
		err = os.WriteFile(destMetricsFile, contents, 0644)
		if err != nil {
			return err
		}
	}
	alertsFile := finderResult.AlertsFile
	if alertsFile == "" {
		return fmt.Errorf("could not find alerts.openmetrics in must-gather")
	}
	destAlertsFile := filepath.Join(workingDir, "alerts.openmetrics")
	if contents, err := os.ReadFile(alertsFile); err == nil {
		err = os.WriteFile(destAlertsFile, contents, 0644)
		if err != nil {
			return err
		}
	}

	const promImage = "docker.io/prom/prometheus"

	// Get the directory where the current executable is located
	exePath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("failed to get executable path: %v", err)
	}
	exeDir := filepath.Dir(exePath)

	// Construct the path to the prometheus.yml file relative to the executable
	// Assuming 'configs' is a sibling directory to 'cmd' or the project root.
	// Adjust this path if your 'configs' directory is nested differently.
	projectRoot := filepath.Join(exeDir, "..", "..") // Adjust if your 'cmd/audit-tool' is not in a 'cmd' dir
	sourcePrometheusConfigPath := filepath.Join(projectRoot, "configs", "prometheus.yml")

	// Ensure the source config file exists
	if _, err := os.Stat(sourcePrometheusConfigPath); os.IsNotExist(err) {
		return fmt.Errorf("prometheus.yml not found at %s. Please create it", sourcePrometheusConfigPath)
	}

	// Copy the prometheus.yml from source to the workingDir
	destPrometheusConfigFile := filepath.Join(workingDir, "prometheus.yml")
	contents, err := os.ReadFile(sourcePrometheusConfigPath)
	if err != nil {
		return fmt.Errorf("failed to read prometheus.yml from %s: %v", sourcePrometheusConfigPath, err)
	}
	err = os.WriteFile(destPrometheusConfigFile, contents, 0644)
	if err != nil {
		return fmt.Errorf("failed to copy prometheus.yml to working directory %s: %v", destPrometheusConfigFile, err)
	}
	log.Printf("Copied prometheus.yml from %s to %s", sourcePrometheusConfigPath, destPrometheusConfigFile)

	// entries, err := os.ReadDir(workingDir)
	// if err != nil {
	// 	return fmt.Errorf("failed to read working dir: %v", err)
	// }
	// log.Println("Working directory contents:")
	// for _, e := range entries {
	// 	log.Println(" -", e.Name())
	// }

	// Convert all OpenMetrics files to Prometheus blocks
	files := []string{auditFile, metricsFile}
	for _, input := range files {
		if err := prometheusMetrics(input, workingDir, promImage); err != nil {
			return err
		}
	}

	// log.Println("Adjusting permissions for Prometheus data directory...")

	// chownCmd := exec.CommandContext(ctx, "docker", "run", "--rm",
	// 	"-v", fmt.Sprintf("%s:/prometheus:z", workingDir), // Mount the directory
	// 	"--entrypoint", "chown", // Use chown command
	// 	promImage,                 // Use the prometheus image as it has chown
	// 	"65534:65534",             // The default user:group in prometheus image
	// 	"/prometheus/prom-blocks") // The directory inside the container

	// chownCmd.Stdout = os.Stdout
	// chownCmd.Stderr = os.Stderr
	// chownCmd.Env = os.Environ()

	// log.Printf("Running chown command: %s %s", chownCmd.Path, strings.Join(chownCmd.Args, " "))
	// if err := chownCmd.Run(); err != nil {
	// 	return fmt.Errorf("failed to adjust permissions for prom-blocks: %v. Command: %s %s", err, chownCmd.Path, strings.Join(chownCmd.Args, " "))
	// }
	// log.Println("Permissions adjusted.")

	log.Println("Prometheus is starting up at http://localhost:9090...")

	cmd := exec.CommandContext(ctx, "podman", "run", "--rm",
		"-p", "9090:9090",
		"--user", "root",
		"-v", fmt.Sprintf("%s:/prometheus:z", workingDir),
		"--entrypoint", "prometheus",
		promImage,
		"--storage.tsdb.path=/prometheus/prom-blocks")
	// fmt.Println("I'M ABOUT TO RUN", cmd.String())
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = os.Environ()
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to start prometheus container: %v", err)
	}

	return nil
}

// Update prometheusMetrics to return error
func prometheusMetrics(inputFile, workingDir string, promImage string) error {
	outputParentDir := filepath.Join(workingDir, "prom-blocks")
	if err := os.MkdirAll(outputParentDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory for prom-blocks: %v", err)
	}

	cmd := exec.Command("podman", "run", "--rm",
		"--user", "0",
		"-v", fmt.Sprintf("%s:/prometheus:z", workingDir),
		"--entrypoint", "promtool",
		promImage,
		"tsdb", "create-blocks-from", "openmetrics",
		"/prometheus/"+filepath.Base(inputFile), "/prometheus/prom-blocks")

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		log.Fatalf("failed to run promtool: %v", err)
	}
	return nil
}
