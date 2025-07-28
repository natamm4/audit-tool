package visualize

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// FinderResult holds the found files
type FinderResult struct {
	AuditLogDir string
	MetricsFile string
	AlertsFile  string
}

// FindMustGatherFiles recursively searches for audit logs and metrics file
func FindMustGatherFiles(root string) (*FinderResult, error) {
	var result FinderResult

	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// Find kube-apiserver audit log directory
		if info.IsDir() && strings.HasSuffix(path, "audit_logs/kube-apiserver") {
			result.AuditLogDir = path
		}
		// Find metrics.openmetrics or metrics.openmetrics.gz file inside any etcd_info directory
		if info.Mode().IsRegular() && (info.Name() == "metrics.openmetrics" || info.Name() == "metrics.openmetrics.gz") && strings.Contains(path, "etcd_info") {
			if info.Name() == "metrics.openmetrics.gz" {
				if err := GunzipFile(path); err != nil {
					return err
				}
				path = strings.TrimSuffix(path, ".gz")
			}
			result.MetricsFile = path
		}
		// Find alerts
		if info.Mode().IsRegular() && info.Name() == "metrics.openmetrics" && strings.Contains(path, "monitoring/alert_metrics") {
			result.AlertsFile = path
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return &result, nil
}

// GunzipFile runs the gunzip command to decompress a .gz file in place
func GunzipFile(gzPath string) error {
	cmd := exec.Command("gunzip", gzPath)
	return cmd.Run()
}
