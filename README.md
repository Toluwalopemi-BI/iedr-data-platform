# IEDR Data Platform

> **Integrated Energy Data Resource** — Production DLT medallion architecture for NY state utility data integration.

[![CI](https://github.com/Toluwalopemi-BI/iedr-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/Toluwalopemi-BI/iedr-data-platform/actions/workflows/ci.yml)

---

## Overview

The IEDR Data Platform ingests, cleanses, unifies, and serves production datasets from New York State's operating utilities. Built on **Databricks Delta Live Tables (DLT)** with a four-layer medallion architecture.

```
Landing Zone (ADLS Gen2 / S3)
    │
    ▼
┌──────────┐     ┌───────────┐     ┌───────────┐     ┌──────────────┐
│  BRONZE  │ ──▶ │  SILVER   │ ──▶ │   GOLD    │ ──▶ │  PLATINUM    │
│ Raw /    │     │ Cleansed  │     │ Unified   │     │ API-Ready    │
│ Append   │     │ Per-Util  │     │ Star      │     │ Z-ORDERed    │
│ Only     │     │ Typed     │     │ Schema    │     │ Views        │
└──────────┘     └───────────┘     └───────────┘     └──────────────┘
  Auto Loader     DLT Expect.       Cross-Util        Serving Layer
  All strings     Dedup / Cast      dim + fact         IEDR API
```

### Datasets per Utility

| Dataset | Description | Key Metrics |
|---------|-------------|-------------|
| **Network (Circuits)** | Feeder infrastructure, hosting capacity | Voltage, Max/Min HC (MW) |
| **Installed DER** | Operational distributed energy resources | Type, Nameplate Rating, Feeder |
| **Planned DER** | Projects in interconnection queue | Status, In-Service Date |

---

## Project Structure

```
iedr-data-platform/
├── .github/workflows/          # CI/CD pipeline definitions
│   ├── ci.yml                  # Lint + Unit Test + Schema Validate
│   ├── cd-test.yml             # Auto-deploy to TEST on develop merge
│   └── cd-prod.yml             # Deploy to PROD on main merge + tag
├── pipelines/                  # DLT pipeline code (medallion layers)
│   ├── bronze/                 # Raw ingestion via Auto Loader
│   ├── silver/                 # Per-utility cleansing & typing
│   ├── gold/                   # Cross-utility unified star schema
│   └── platinum/               # API-ready materialized views
├── config/                     # Data-driven configuration
│   ├── utility_registry.yaml   # Column mappings per utility
│   ├── schema_contracts.yaml   # Expected schemas per layer
│   └── der_type_mapping.yaml   # Canonical DER type lookup
├── tests/                      # Automated test suite
│   ├── unit/                   # PySpark unit tests (no cluster needed)
│   └── integration/            # Full pipeline validation
├── shared/                     # Reusable transformation utilities
├── databricks.yml              # Databricks Asset Bundle definition
└── pyproject.toml              # Python project + tooling config
```

## Environments

| Environment | Catalog | Branch | Deployment |
|-------------|---------|--------|------------|
| **DEV** | `iedr_dev` | `feature/*` | Manual: `databricks bundle deploy -t dev` |
| **TEST** | `iedr_test` | `develop` | Auto CD on merge to develop |
| **PROD** | `iedr_prod` | `main` | Auto CD on tagged release to main |

## Quick Start

```bash
# Install Databricks CLI
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# Configure auth profile
databricks configure --profile iedr-dev

# Validate, deploy, run
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run iedr_dlt_pipeline -t dev
```

## Branch Strategy (Gitflow)

| Branch | Purpose | Deploys To |
|--------|---------|------------|
| `main` | Production truth, tagged releases | PROD (2 approvers) |
| `develop` | Integration baseline | TEST (auto) |
| `feature/IEDR-*` | Individual work items | DEV (manual) |
| `release/v*` | Release stabilization | TEST (QA) |
| `hotfix/IEDR-*` | Emergency production fixes | PROD (fast-track) |

## License

Proprietary — All rights reserved.
