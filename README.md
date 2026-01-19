# Terra-Cost Data Pipeline Architecture

## Overview

The terra-cost project implements a **multi-cloud pricing ingestion and resolution pipeline** designed for production-grade Terraform cost estimation. The architecture emphasizes **strict isolation**, **atomic commits**, and **memory efficiency**.

```mermaid
flowchart TB
    subgraph Sources["Cloud Pricing APIs"]
        AWS_API["AWS Pricing API"]
        Azure_API["Azure Retail Prices API"]
        GCP_API["GCP Cloud Billing API"]
    end

    subgraph Registry["Fetcher Registry"]
        FR["FetcherRegistry"]
        FR --> AWS_F["AWSPricingAPIFetcher"]
        FR --> Azure_F["AzurePricingAPIClient"]
        FR --> GCP_F["GCPPricingAPIClient"]
    end

    subgraph Pipeline["Ingestion Pipeline"]
        direction TB
        P1["Phase 1: Fetching<br/>NO DB ACCESS"]
        P2["Phase 2: Normalizing<br/>NO DB ACCESS"]
        P3["Phase 3: Validating<br/>NO DB ACCESS"]
        P4["Phase 4: Staging<br/>NO DB ACCESS"]
        P5["Phase 5: Backup<br/>WRITE TO DISK"]
        P6["Phase 6: Committing<br/>ATOMIC TX"]
        P7["Phase 7: Active<br/>RESOLVER READY"]
        
        P1 --> P2 --> P3 --> P4 --> P5 --> P6 --> P7
    end

    subgraph Storage["PostgreSQL Storage"]
        Snapshots["pricing_snapshots"]
        RateKeys["rate_keys"]
        Rates["pricing_rates"]
        Snapshots --> Rates
        RateKeys --> Rates
    end

    subgraph Resolution["Cost Resolution"]
        Resolver["Pricing Resolver"]
        Tiered["Tiered Rate Calculator"]
    end

    Sources --> Registry
    Registry --> Pipeline
    Pipeline --> Storage
    Storage --> Resolution
```

---

## Core Components

### 1. Cloud Provider Fetchers

Location: [db/ingestion/](db/ingestion/)

| Provider | Fetcher | Normalizer | API Source |
|----------|---------|------------|------------|
| **AWS** | `AWSPricingAPIFetcher` | `AWSPricingAPINormalizer` | AWS Bulk Price List JSON |
| **Azure** | `AzurePricingAPIClient` | `AzurePricingNormalizer` | Azure Retail Prices REST API |
| **GCP** | `GCPPricingAPIClient` | `GCPPricingNormalizer` | GCP Cloud Billing Catalog API |

> [!NOTE]
> All fetchers now implement strict production guards. The `IsRealAPI()` method ensures no stub data can be used in production environments.

#### Fetcher Interface
```go
type PriceFetcher interface {
    Cloud() db.CloudProvider
    FetchRegion(ctx context.Context, region string) ([]RawPrice, error)
    SupportedRegions() []string
    SupportedServices() []string
}
```

#### Normalizer Interface
```go
type PriceNormalizer interface {
    Cloud() db.CloudProvider
    Normalize(raw []RawPrice) ([]NormalizedRate, error)
}
```

---

### 2. Fetcher Registry

Location: [registry.go](db/ingestion/registry.go)

The `FetcherRegistry` provides **centralized, thread-safe access** to cloud fetchers and normalizers:

```mermaid
classDiagram
    class FetcherRegistry {
        -mu sync.RWMutex
        -fetchers map[CloudProvider]PriceFetcher
        -normalizers map[CloudProvider]PriceNormalizer
        +GetRegistry() *FetcherRegistry
        +GetFetcher(cloud) PriceFetcher
        +GetNormalizer(cloud) PriceNormalizer
        +RegisterDefaults()
        +IsRealAPI(cloud) bool
    }
    
    FetcherRegistry --> AWSPricingAPIFetcher
    FetcherRegistry --> AzurePricingAPIClient
    FetcherRegistry --> GCPPricingAPIClient
```

**Key Features:**
- Singleton pattern via `GetRegistry()`
- Auto-registration of production API clients
- Distinguishes real APIs from stub/mock implementations

---

### 3. Ingestion Lifecycle State Machine

Location: [lifecycle.go](db/ingestion/lifecycle.go)

The ingestion follows a **strict 8-phase state machine** with hard isolation guarantees:

```mermaid
stateDiagram-v2
    [*] --> Init
    Init --> Fetching: Start
    Fetching --> Normalizing: Raw prices fetched
    Normalizing --> Validating: Canonical form ready
    Validating --> Staging: Governance passed
    Staging --> BackedUp: Backup written & verified
    BackedUp --> Committing: Ready for DB
    Committing --> Active: Atomic commit success
    
    Fetching --> Failed: Error
    Normalizing --> Failed: Error
    Validating --> Failed: Coverage < threshold
    Staging --> Failed: Error
    BackedUp --> Failed: Backup verification failed
    Committing --> Failed: Transaction error
    
    Failed --> [*]
    Active --> [*]
```

| Phase | Description | DB Access |
|-------|-------------|-----------|
| `Init` | Initialize lifecycle | ❌ None |
| `Fetching` | Download from cloud APIs | ❌ None |
| `Normalizing` | Transform to canonical form | ❌ None |
| `Validating` | Run governance checks | ❌ None |
| `Staging` | Prepare for commit | ❌ None |
| `BackedUp` | Write & verify backup file | ❌ Disk only |
| `Committing` | Atomic DB transaction | ✅ Single TX |
| `Active` | Resolver can use snapshot | ✅ Read only |

> [!IMPORTANT]
> **NO database writes occur until Phase 6 (Committing)**. This ensures failed ingestions leave no partial state.

---

### 4. Streaming Pipeline (Low-Memory Mode)

Location: [streaming.go](db/ingestion/streaming.go)

For memory-constrained environments (4-8GB RAM), the `StreamingLifecycle` provides:

```mermaid
flowchart LR
    subgraph Fetch["Streaming Fetch"]
        S1["Service 1"] --> TF1["temp_s1.json.gz"]
        S2["Service 2"] --> TF2["temp_s2.json.gz"]
        S3["Service N"] --> TF3["temp_sN.json.gz"]
    end
    
    subgraph Merge["Merge & Validate"]
        TF1 --> MV["Merge Validator"]
        TF2 --> MV
        TF3 --> MV
    end
    
    subgraph Commit["Batch Commit"]
        MV --> B1["Batch 1<br/>5000 rates"]
        MV --> B2["Batch 2<br/>5000 rates"]
        MV --> B3["Batch N<br/>remaining"]
    end
    
    B1 --> DB[(PostgreSQL)]
    B2 --> DB
    B3 --> DB
```

| Memory Profile | Batch Size | Max Memory | GC Interval |
|----------------|------------|------------|-------------|
| `LowMemoryConfig` | 2,000 | 512 MB | Every 2 batches |
| `DefaultStreamingConfig` | 5,000 | 1024 MB | Every 5 batches |
| `HighMemoryConfig` | 20,000 | 4096 MB | Every 10 batches |

**Checkpoint & Resume:**
- Progress written to `checkpoint.json` after each service
- Resumes from last completed service on restart
- Temp files use gzip compression

---

### 5. Data Model & Storage

Location: [types.go](db/types.go), [postgres.go](db/postgres.go)

```mermaid
erDiagram
    pricing_snapshots ||--o{ pricing_rates : contains
    rate_keys ||--o{ pricing_rates : references
    
    pricing_snapshots {
        uuid id PK
        string cloud
        string region
        string provider_alias
        string source
        timestamp fetched_at
        timestamp valid_from
        timestamp valid_to
        string hash
        boolean is_active
    }
    
    rate_keys {
        uuid id PK
        string cloud
        string service
        string product_family
        string region
        jsonb attributes
    }
    
    pricing_rates {
        uuid id PK
        uuid snapshot_id FK
        uuid rate_key_id FK
        string unit
        decimal price
        string currency
        float confidence
        decimal tier_min
        decimal tier_max
    }
```

**Key Design Decisions:**
- **Snapshot-based versioning**: Each ingestion creates immutable snapshot
- **Content hashing**: Detects unchanged pricing (skips redundant commits)
- **Tiered pricing support**: `tier_min`/`tier_max` for S3, data transfer, etc.
- **Confidence scoring**: 0.0-1.0 rating for price reliability

---

### 6. Pricing Resolver

Location: [resolver.go](db/resolver.go)

The `Resolver` provides the interface between the estimation engine and the pricing database:

```go
type Resolver struct {
    store        PricingStore
    defaultAlias string
    strictMode   bool
}

type ResolveRequest struct {
    Cloud         CloudProvider
    Service       string
    ProductFamily string
    Region        string
    Attributes    map[string]string
    Unit          string
    Alias         string
}
```

**Resolution Flow:**
1. Get active snapshot for cloud/region/alias
2. Match rate key by service + product family + attributes
3. Return price with confidence score
4. Support tiered pricing calculation

**Modes:**
- **Normal**: Returns symbolic result if rate not found
- **Strict**: Fails hard on missing rates (for testing)

---

### 7. Region Registry

Location: [regions/registry.go](db/regions/registry.go)

Centralized source of truth for billable regions across all cloud providers:

| Provider | Billable Regions | Pricing Source |
|----------|------------------|----------------|
| AWS | 33 regions | Bulk JSON API |
| Azure | 60+ regions | Retail Prices API |
| GCP | 35+ regions | Cloud Billing API |

---

### 8. Database Migrations

Location: [migrations/](db/migrations/)

| Migration | Purpose |
|-----------|---------|
| `001_pricing_schema.sql` | Core tables: snapshots, rate_keys, rates |
| `002_pricing_dimensions.sql` | Dimension expansion for attributes |
| `003_scale_hardening.sql` | Indexes, partitioning for scale |
| `004_snapshot_lifecycle.sql` | Lifecycle state tracking |
| `005_region_aliases.sql` | Multi-alias support per region |

---

## Data Flow Summary

```mermaid
sequenceDiagram
    participant CLI as CLI/Scheduler
    participant Lifecycle as Lifecycle Manager
    participant Fetcher as Cloud Fetcher
    participant Normalizer as Price Normalizer
    participant Backup as Backup Writer
    participant DB as PostgreSQL
    participant Resolver as Pricing Resolver

    CLI->>Lifecycle: Execute(config)
    activate Lifecycle
    
    Lifecycle->>Fetcher: FetchRegion(region)
    Fetcher-->>Lifecycle: []RawPrice
    
    Lifecycle->>Normalizer: Normalize(rawPrices)
    Normalizer-->>Lifecycle: []NormalizedRate
    
    Lifecycle->>Lifecycle: Validate(coverage, governance)
    
    Lifecycle->>Backup: WriteBackup(rates)
    Backup-->>Lifecycle: backupPath
    
    Lifecycle->>DB: BeginTx()
    Lifecycle->>DB: CreateSnapshot()
    Lifecycle->>DB: BulkCreateRates()
    Lifecycle->>DB: ActivateSnapshot()
    Lifecycle->>DB: Commit()
    
    deactivate Lifecycle
    
    Note over CLI,Resolver: Later, during cost estimation...
    
    CLI->>Resolver: Resolve(request)
    Resolver->>DB: GetActiveSnapshot()
    Resolver->>DB: ResolveRate()
    DB-->>Resolver: ResolvedRate
    Resolver-->>CLI: price + confidence
```

---

## Key Architectural Properties

| Property | Implementation |
|----------|----------------|
| **Atomicity** | Single DB transaction in commit phase |
| **Isolation** | No DB writes until validated & backed up |
| **Durability** | Mandatory backup before commit |
| **Idempotency** | Content hash prevents duplicate snapshots |
| **Recoverability** | Checkpointing enables resume after failure |
| **Memory Efficiency** | Streaming mode with batched commits |
| **Multi-Cloud** | Pluggable fetcher/normalizer architecture |

---

## Deployment & Configuration

### Containerization

The application is fully containerized with a supporting PostgreSQL database.

**Dockerfile**:
- Multi-stage build (`golang:1.25` -> `alpine`)
- Includes `ca-certificates` for secure API access
- Runs as a standalone binary `terracost`

**Docker Compose**:
- Orchestrates `app` and `postgres` services
- Health checks ensure database readiness before app start
- Volume mapping for persistent data and backups

### Configuration Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_URL` | PostgreSQL connection string | *Required* |
| `CLOUD` | Cloud provider (`aws`, `azure`, `gcp`) | `aws` |
| `REGION` | Target region code | `us-east-1` |
| `SERVICES` | Comma-separated list of services to fetch | *All* |
| `BACKUP_DIR` | Directory for ingestion backups | `/app/backups` |

### Development Mode

For rapid development, you can filter specific services to speed up ingestion:

```yaml
environment:
  - SERVICES=AmazonEC2,AmazonS3 # Only download EC2 and S3 pricing
```

This bypasses the full catalog download, significantly reducing ingestion time and memory usage.

---

## Verification Commands

### 1. Run with Docker (Recommended)
This spins up the app and a fresh Postgres DB:
```powershell
docker-compose up --build
```
*Look for "Ingestion completed successfully!" in the logs.*

### 2. Run locally (Requires DB)
If you have a local Postgres running:
```powershell
# Windows PowerShell
$env:DB_URL="postgres://user:pass@localhost:5432/terracost?sslmode=disable"
$env:SERVICES="AmazonS3"  # Test with just S3 for speed
go run ./cmd/terracost
```

### 3. Run Unit Tests
Verify the core logic and safety guards:
```powershell
go test ./db/... -v
```
