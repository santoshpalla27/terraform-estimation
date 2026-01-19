// Package ingestion - Backup and restore for pricing snapshots
package ingestion

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"terraform-cost/db"
)

// SnapshotBackup is a complete snapshot dump for backup/restore
type SnapshotBackup struct {
	// Provider is the cloud provider
	Provider db.CloudProvider `json:"provider"`

	// Region
	Region string `json:"region"`

	// Alias for multi-account
	Alias string `json:"alias"`

	// Timestamp when backup was created
	Timestamp time.Time `json:"timestamp"`

	// ContentHash of the rates
	ContentHash string `json:"content_hash"`

	// RateCount for quick validation
	RateCount int `json:"rate_count"`

	// SchemaVersion for compatibility
	SchemaVersion string `json:"schema_version"`

	// Rates is the complete set of normalized rates
	Rates []NormalizedRate `json:"rates"`
}

// BackupManager handles backup creation and reading
type BackupManager struct{}

// NewBackupManager creates a backup manager
func NewBackupManager() *BackupManager {
	return &BackupManager{}
}

// WriteBackup writes a snapshot backup to disk
func (m *BackupManager) WriteBackup(baseDir string, backup *SnapshotBackup) (string, error) {
	// Create directory structure: baseDir/provider/timestamp.json.gz
	providerDir := filepath.Join(baseDir, string(backup.Provider))
	if err := os.MkdirAll(providerDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create backup directory: %w", err)
	}

	// Generate filename with timestamp
	filename := fmt.Sprintf("%s_%s.json.gz",
		backup.Region,
		backup.Timestamp.Format("2006-01-02T15-04-05"),
	)
	fullPath := filepath.Join(providerDir, filename)

	// Create file
	file, err := os.Create(fullPath)
	if err != nil {
		return "", fmt.Errorf("failed to create backup file: %w", err)
	}
	defer file.Close()

	// Write gzipped JSON
	gzWriter := gzip.NewWriter(file)
	defer gzWriter.Close()

	encoder := json.NewEncoder(gzWriter)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(backup); err != nil {
		return "", fmt.Errorf("failed to write backup: %w", err)
	}

	return fullPath, nil
}

// ReadBackup reads a snapshot backup from disk
func (m *BackupManager) ReadBackup(path string) (*SnapshotBackup, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open backup file: %w", err)
	}
	defer file.Close()

	var reader io.Reader = file

	// Handle gzipped files
	if strings.HasSuffix(path, ".gz") {
		gzReader, err := gzip.NewReader(file)
		if err != nil {
			return nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzReader.Close()
		reader = gzReader
	}

	var backup SnapshotBackup
	if err := json.NewDecoder(reader).Decode(&backup); err != nil {
		return nil, fmt.Errorf("failed to decode backup: %w", err)
	}

	// Validate
	if err := m.ValidateBackup(&backup); err != nil {
		return nil, fmt.Errorf("backup validation failed: %w", err)
	}

	return &backup, nil
}

// ValidateBackup validates a backup file
func (m *BackupManager) ValidateBackup(backup *SnapshotBackup) error {
	if backup.Provider == "" {
		return fmt.Errorf("backup missing provider")
	}
	if backup.Region == "" {
		return fmt.Errorf("backup missing region")
	}
	if backup.ContentHash == "" {
		return fmt.Errorf("backup missing content hash")
	}
	if backup.RateCount == 0 {
		return fmt.Errorf("backup has 0 rates")
	}
	if len(backup.Rates) != backup.RateCount {
		return fmt.Errorf("backup rate count mismatch: header says %d, actual %d", backup.RateCount, len(backup.Rates))
	}

	// Verify hash
	actualHash := calculateHash(backup.Rates)
	if actualHash != backup.ContentHash {
		return fmt.Errorf("backup content hash mismatch: expected %s, got %s", backup.ContentHash, actualHash)
	}

	return nil
}

// ListBackups lists all backups in a directory
func (m *BackupManager) ListBackups(baseDir string) ([]BackupInfo, error) {
	var backups []BackupInfo

	providers := []string{"aws", "azure", "gcp"}
	for _, provider := range providers {
		providerDir := filepath.Join(baseDir, provider)
		if _, err := os.Stat(providerDir); os.IsNotExist(err) {
			continue
		}

		entries, err := os.ReadDir(providerDir)
		if err != nil {
			continue
		}

		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			if !strings.HasSuffix(entry.Name(), ".json") && !strings.HasSuffix(entry.Name(), ".json.gz") {
				continue
			}

			info, err := entry.Info()
			if err != nil {
				continue
			}

			backups = append(backups, BackupInfo{
				Provider:  db.CloudProvider(provider),
				Path:      filepath.Join(providerDir, entry.Name()),
				Filename:  entry.Name(),
				Size:      info.Size(),
				CreatedAt: info.ModTime(),
			})
		}
	}

	return backups, nil
}

// BackupInfo is metadata about a backup file
type BackupInfo struct {
	Provider  db.CloudProvider `json:"provider"`
	Path      string           `json:"path"`
	Filename  string           `json:"filename"`
	Size      int64            `json:"size"`
	CreatedAt time.Time        `json:"created_at"`
}
