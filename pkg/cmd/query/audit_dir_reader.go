package query

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type AuditDirReader struct {
	files map[string][]auditFile
}

type auditFile struct {
	name      string
	filePath  string
	timestamp time.Time
}

func NewAuditDirReader(dir string) (*AuditDirReader, error) {
	dirStat, err := os.Stat(dir)
	if os.IsNotExist(err) {
		return nil, err
	}
	if !dirStat.IsDir() {
		return nil, fmt.Errorf("not a directory %q", dir)
	}

	auditFiles := []auditFile{}

	if err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		// TODO: make this recursive
		if info.IsDir() {
			return nil
		}
		// audit files only
		if !strings.Contains(info.Name(), "-audit") {
			return nil
		}
		auditFiles = append(auditFiles, auditFile{
			name:      info.Name(),
			filePath:  filepath.Join(dir, info.Name()),
			timestamp: parseTimeFromRotatedAuditFile(info.Name(), info.ModTime()),
		})
		return nil
	}); err != nil {
		return nil, err
	}

	sort.Slice(auditFiles, func(i, j int) bool {
		return auditFiles[i].timestamp.After(auditFiles[j].timestamp)
	})

	// now map the audit files to nodes
	files := map[string][]auditFile{}
	for _, f := range auditFiles {
		parts := strings.Split(f.name, "-audit")
		nodeName := parts[0]
		if _, ok := files[nodeName]; !ok {
			files[nodeName] = []auditFile{f}
		} else {
			files[nodeName] = append(files[nodeName], f)
		}
	}

	return &AuditDirReader{files: files}, nil
}

func parseTimeFromRotatedAuditFile(name string, modTime time.Time) time.Time {
	parts := strings.Split(name, "-audit-")
	utcTime, err := time.LoadLocation("UTC")
	if err != nil {
		panic(err)
	}
	if len(parts) != 2 {
		return modTime.In(utcTime)
	}
	timeString := strings.TrimSuffix(parts[1], ".log.gz")
	timeT, err := time.Parse("2006-01-02T15-04-05.000", timeString)
	if err != nil {
		return modTime.In(utcTime)
	}
	return timeT
}
