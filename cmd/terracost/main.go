package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"terraform-cost/db"
	"terraform-cost/db/ingestion"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
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

	// 2a. Run Migrations
	if err := runMigrations(dbURL); err != nil {
		return fmt.Errorf("migration failed: %w", err)
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

func runMigrations(dbURL string) error {
	// Look for migrations in /app/migrations (docker) or ./db/migrations (local)
	sourceURL := "file://db/migrations"
	if _, err := os.Stat("/app/migrations"); err == nil {
		sourceURL = "file:///app/migrations"
	}

	fmt.Printf("Running migrations from %s...\n", sourceURL)
	
	m, err := migrate.New(sourceURL, dbURL)
	if err != nil {
		return fmt.Errorf("failed to create migrate instance: %w", err)
	}
	defer m.Close()

	if err := m.Up(); err != nil {
		if err == migrate.ErrNoChange {
			return nil
		}
		
		fmt.Printf("Migration failed: %v\n", err)

		// Check for dirty state
		version, dirty, verErr := m.Version()
		if verErr != nil {
			return fmt.Errorf("failed to get migration version: %w", verErr)
		}

		if dirty && os.Getenv("APP_ENV") != "production" {
			fmt.Printf("Detected dirty migration at version %d. Attempting auto-heal in dev env...\n", version)
			
			// Force previous version (heuristic: assuming 1 step failure)
			// Simplification: just force version-1. If version is 1, force 0.
			forceVersion := int(version) - 1
			if forceVersion < 0 {
				forceVersion = 0
			}

			if forceErr := m.Force(forceVersion); forceErr != nil {
				return fmt.Errorf("failed to force clean dirty state: %w", forceErr)
			}

			fmt.Printf("Successfully forced version %d. Please fix the migration file and restart.\n", forceVersion)
			// We intentionally exit here so the user can see the error of the *reason* it failed (the SQL error)
			// If we retried immediately, it would likely just fail again with the same syntax error.
			return fmt.Errorf("auto-healed dirty state. Fix migration SQL and restart: %w", err)
		}
		
		return fmt.Errorf("failed to run up migrations: %w", err)
	}

	fmt.Println("Database migrations applied successfully")
	return nil
}
