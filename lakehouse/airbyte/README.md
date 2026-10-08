# ⚡ Airbyte Data Lakehouse - Secure DB Provisioning & Ingestion Tool

A standalone, zero-dependency Go application engineered to safely onboard external and customer relational databases (PostgreSQL, MySQL, SQL Server) into an enterprise **MinIO-backed Data Lakehouse** via **Airbyte CDC (Change Data Capture)** and **Parquet landing (`staging/`)**.

---

## 📌 Architecture Overview

This tool bridges the gap between **Customer DBAs** (who prioritize database security and isolation) and **Data Platform Engineers** (who automate Airbyte pipelines and object storage ingestion).

```
┌────────────────────────────────────────┐            ┌──────────────────────────────────────────────┐
│        CUSTOMER / DBA ENVIRONMENT      │            │       CENTRAL DATA PLATFORM SERVER           │
│                                        │            │                                              │
│  [Source DB: Postgres / MySQL / MSSQL] │            │  [Airbyte OSS Platform]                      │
│            ▲                           │            │  - Web UI & API (:8000)                      │
│            │ (Least-privilege DDL)     │            │               ▲                              │
│            │                           │            │               │ (Automated API Calls)        │
│  [db-provisioner.exe (UI Mode :8085)]  │            │  [db-provisioner bridge (:9090)]             │
│  - Table/Column Discovery & PII Mask   │            │  - Receives bundles from client              │
│  - Generates airbyte_reader user       │            │  - Configures Source, Dest, Connection      │
│  - Exports airbyte_bundle.json         │            │               │                              │
│            │                           │            │               ▼                              │
│            └──────── (HTTP / Sync) ─────────────────┼─▶ [MinIO Object Storage (:9000, :9001)]      │
│                      or Manual JSON Handoff         │   - Bucket: university-lakehouse             │
│                                                     │   - Prefix: staging/<tenant_id>/             │
│                                                     │   - Snappy Compressed Parquet (.parquet)     │
└────────────────────────────────────────┘            └──────────────────────────────────────────────┘
```

---

## 🌐 Network Ports & Service Reference

Below are all the ports used across this tool and its connected ecosystem:

| Port | Service Name | Protocol | Bound Component | Purpose & Description |
| :---: | :---: | :---: | :---: | :--- |
| **`8085`** | **Web UI Wizard & Parquet Explorer** | HTTP | `db-provisioner.exe ui` (or default run) | **Client / Local UI**: Interactive 7-step onboarding wizard for DBAs, plus the Parquet SCD Type 2 Version History Explorer. Automatically launches default web browser on start. |
| **`9090`** | **Airbyte Platform API Bridge** | HTTP | `db-provisioner.exe bridge` | **Server-side Gateway**: Listens for incoming onboarding bundles (`/api/v1/airbyte/onboard-source`). Automates Airbyte Source/Destination/Connection provisioning via OAuth2. |
| **`8000`** | **Airbyte OSS Platform** | HTTP | Docker (`airbyte-abctl-control-plane`) | Core Airbyte data orchestration engine (Web UI & Public REST API v1). |

---

## 🚀 Key Features

1. **Zero External Dependencies:**
   * Pure Go implementation. All database drivers (`pgx`, `go-sql-driver/mysql`, `go-mssqldb`) and MinIO/Parquet libraries are compiled statically.
   * Full HTML/CSS/JS frontend embedded directly inside the binary via Go `embed.FS`.
   * Double-clicking `db-provisioner.exe` immediately launches the local server and opens the browser.

2. **Least-Privilege & Zero-Write DDL Generation:**
   * Generates strict, hardened SQL scripts for customer DBAs.
   * Creates an isolated service account (`airbyte_reader`) with explicit `USAGE` and `SELECT` only on selected physical tables or vault views.
   * Injects database-level guardrails: `statement_timeout = '30s'` and `default_transaction_read_only = on`.
   * Configures native CDC replication slots and publications with disk-bloat limits (`max_slot_wal_keep_size = 20GB`).

3. **Multi-Tenant Lakehouse Isolation:**
   * Dynamic duplicate tenant detection against live MinIO buckets (`/api/check-tenant`).
   * Clean, deterministic landing directory structure:
     ```text
     s3://university-lakehouse/staging/<tenant_id>/<table_name>/part_0.parquet
     ```

4. **Automated Airbyte Bridge (Zero Manual Setup):**
   * Eliminates manual Airbyte UI clicking.
   * Auto-configures PostgreSQL/MySQL CDC sources (`snapshot_mode: initial`).
   * Auto-configures MinIO S3 Parquet destinations.
   * Establishes connections and immediately triggers the initial sync job.
   * Watches the Airbyte job and, after it succeeds, triggers the generic Airflow
     context pipeline with `context_id` and `airbyte_job_id`.

5. **Parquet SCD Type 2 & Version History Explorer:**
   * Full-screen data table browser for Parquet files stored in MinIO.
   * Collapsible row version history showing prior versions and transition states (`UPDATE`, `DELETE`).
   * Hard-deleted record indicators highlighted in red.

---

## 🛠️ Build Instructions

### For Windows:
Double-click `build.bat` or run in PowerShell:
```powershell
$env:CGO_ENABLED="0"
go build -ldflags="-s -w" -o db-provisioner.exe ./cmd/db-provisioner
```

### For Linux / macOS:
Run the provided build script:
```bash
chmod +x build.sh
./build.sh
```
Or manually:
```bash
CGO_ENABLED=0 go build -ldflags="-s -w" -o db-provisioner ./cmd/db-provisioner
```

---

## 💻 Operating Modes

### Mode 1: Client Web UI Wizard (Default)
Run without arguments (or double-click the `.exe`):
```bash
./db-provisioner.exe
# Or explicitly:
./db-provisioner.exe ui --web-port 8085
```
* **Step 1:** Connect to source DB & specify Tenant ID (with real-time duplicate check).
* **Step 2:** Select tables, columns, and configure PII masking.
* **Step 3:** Select Sync Strategy (Standard Incremental vs. Controlled CDC).
* **Step 4:** Review generated DDL and publication statements.
* **Step 5 & 6:** Execute DDL, verify reader permissions, and download DBA audit queries.
* **Step 7:** Download `airbyte_bundle_<tenant>.json` or trigger direct sync via Bridge.

### Mode 2: Server-side Airbyte Bridge Daemon
Run on the server hosting Airbyte OSS and MinIO:
```bash
./db-provisioner.exe bridge --port 9090 \
  --airflow-url http://localhost:8080 \
  --airflow-user airflow \
  --airflow-password admin
```
* Acts as an automated coordinator between client tools and Airbyte OSS at `http://localhost:8000`.
* Airflow values can also be supplied through `AIRFLOW_WEBSERVER_URL`,
  `_AIRFLOW_WWW_USER_USERNAME`, and `AIRFLOW_ADMIN_PASSWORD`.

---

## 👥 Team Collaboration & Remote Testing (Tailscale VPN)

When team members clone this repo and want to test against a central Airbyte/MinIO instance without opening public router ports:

1. **Both the Host and Member install [Tailscale](https://tailscale.com/)** and join the same mesh network.
2. **On Host Machine (Central Server):**
   * Keep the Bridge running: `.\db-provisioner.exe bridge --port 9090`.
   * Find Host Tailscale IP: `tailscale ip -4` (e.g. `100.96.150.68`).
3. **On Member Machine (Testing Local DB):**
   * Start local database (e.g. Docker PostgreSQL `-p 5432:5432`).
   * Find Member Tailscale IP: `tailscale ip -4` (e.g. `100.95.40.25`).
   * Launch `db-provisioner.exe`:
     * **Step 1:** Enter Member Tailscale IP (`100.95.40.25`) as DB Host (*do not use `localhost`*).
     * **Step 7:** Set **Airbyte Coordinator URL** to Host Tailscale IP: `http://100.96.150.68:9090`.
     * Click **"Connect Airbyte & Ingest to Parquet"**.

---

## 📂 Project Directory Structure

```text
data-lakehouse-airbyte-config-tool/
├── cmd/
│   └── db-provisioner/      # CLI entrypoint (main.go with Cobra commands)
├── internal/
│   ├── adapter/             # DB dialect adapters (PostgreSQL, MySQL, MSSQL)
│   ├── airbyte/             # Airbyte Public API client & Bridge HTTP server
│   ├── exporter/            # Handover bundle (.json) & audit SQL generator
│   ├── lakehouse/           # MinIO S3 & DuckDB Parquet inspection engine
│   ├── model/               # Core data structures, schemas, and configurations
│   └── server/              # Local HTTP API server for Web UI & Parquet Explorer
├── web/
│   ├── embed.go             # Go embed.FS static asset binding
│   └── index.html           # Full interactive UI (Tailwind CSS, Vanilla JS)
├── build.bat                # One-click Windows build script
├── build.sh                 # One-click Linux/macOS build script
├── .gitignore               # Excludes binaries (*.exe) and exported secret bundles
└── README.md                # Project documentation
```
