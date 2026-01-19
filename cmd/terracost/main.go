package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"terraform-cost/db"
	"terraform-cost/db/ingestion"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// 1. Configuration from Environment
	dbURL := os.Getenv("DB_URL")
	if dbURL == "" {
		return fmt.Errorf("DB_URL environment variable is required")
	}

	cloud := db.CloudProvider(os.Getenv("CLOUD"))
	if cloud == "" {
		cloud = db.AWS
	}

	region := os.Getenv("REGION")
	if region == "" {
		region = "us-east-1"
	}

	// 2. Connect to Database
	store, err := db.NewPostgresStoreFromURL(dbURL)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer store.Close()

	// Wait for DB to be potentially ready (retry logic)
	ctx := context.Background()
	for i := 0; i < 30; i++ {
		if err := store.Ping(ctx); err == nil {
			fmt.Println("Connected to database successfully")
			break
		}
		fmt.Printf("Waiting for database... (%d/30)\n", i+1)
		time.Sleep(1 * time.Second)
	}

	// 3. Initialize Registry & Fetchers
	registry := ingestion.GetRegistry()
	
	fetcher, err := registry.GetFetcher(cloud)
	if err != nil {
		return fmt.Errorf("failed to get fetcher: %w", err)
	}

	// Filter services for dev environment if requested
	if servicesEnv := os.Getenv("SERVICES"); servicesEnv != "" {
		// Define interface for configurable fetchers to avoid direct typing if possible,
		// or type assert if we know the concrete type for now.
		type ServiceConfigurable interface {
			SetAllowedServices(services []string)
		}

		if configurable, ok := fetcher.(ServiceConfigurable); ok {
			servicesList := strings.Split(servicesEnv, ",")
			fmt.Printf("Filtering fetch to %d services: %v\n", len(servicesList), servicesList)
			configurable.SetAllowedServices(servicesList)
		} else {
			fmt.Printf("Warning: Fetcher for %s does not support service filtering\n", cloud)
		}
	}

	normalizer, err := registry.GetNormalizer(cloud)
	if err != nil {
		return fmt.Errorf("failed to get normalizer: %w", err)
	}

	// 4. Setup Lifecycle
	// Ensure backup directory exists
	backupDir := os.Getenv("BACKUP_DIR")
	if backupDir == "" {
		backupDir = "/app/backups"
	}
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return fmt.Errorf("failed to create backup dir: %w", err)
	}

	lifecycle := ingestion.NewLifecycle(fetcher, normalizer, store)

	config := ingestion.DefaultLifecycleConfig()
	config.Provider = cloud
	config.Region = region
	config.BackupDir = backupDir
	config.Environment = "production"

	// 5. Execute Pipeline
	fmt.Printf("Starting ingestion for %s/%s...\n", cloud, region)
	result, err := lifecycle.Execute(ctx, config)
	if err != nil {
		return fmt.Errorf("ingestion failed: %w", err)
	}

	fmt.Printf("Ingestion completed successfully!\n")
	fmt.Printf("Snapshot ID: %s\n", result.SnapshotID)
	fmt.Printf("Duration: %s\n", result.Duration)
	fmt.Printf("Rates: %d\n", result.NormalizedCount)

	return nil
}
