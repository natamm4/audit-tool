package visualize

import (
	"os"
	"path/filepath"
	"strings"
)

// FinderResult holds the found files
type FinderResult struct {
	AuditLogDir string
	MetricsFile string
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
		// Find etcd and alert metrics file
		if info.Mode().IsRegular() && info.Name() == "metrics.openmetrics" && strings.Contains(path, "monitoring/alert_metrics") {
			result.MetricsFile = path
		} else if info.Mode().IsRegular() && info.Name() == "metrics.openmetrics" && strings.Contains(path, "etcd_info") {
			result.MetricsFile = path
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return &result, nil
}
