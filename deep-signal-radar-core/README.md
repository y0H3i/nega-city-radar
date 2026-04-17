# Deep-Signal-Radar (Core Engine)

Deep-Signal-Radar is the autonomous backend ingestion engine for the "Nega-City" architecture. 
It is designed to filter out the overwhelming "noise" of late-stage capitalism and extract high-purity, 100% pure "signals" regarding AI, distributed systems, and novel software architectures.

## Architecture & Algorithm

This system employs a strict vertical-slice architecture, separating concerns across data ingestion, AI-driven evaluation, persistence, and API routing.

### 1. Data Ingestion (Python)
- **Source:** Scrapes the latest articles and discussions from the Hacker News RSS feed.
- **Algorithm:** Parses the XML feed, extracting URLs, titles, and metadata for the most recent discussions.

### 2. AI Extraction & Filtering Layer (Google Gemini 2.5 Flash)
- **Engine:** Utilizes `gemini-2.5-flash` for high-speed, high-accuracy context evaluation.
- **Scoring Algorithm:** The LLM is instructed to act as a strict evaluator. It reads the context of each article and assigns a `signal_score` (1-10). 
- **Threshold:** Only articles scoring `7 or higher` (indicating novel architectures, significant AI developments, or paradigm shifts) are extracted. The LLM formats the output strictly as a structured JSON array containing the score, a reasoned explanation, and a summary.

### 3. Orchestration & API Routing (Go)
- **Subprocess Management:** A lightweight Go application acts as the orchestrator. It runs a background `time.Ticker` (Ingestion Loop) that executes the Python extraction pipeline as a subprocess.
- **API Gateway:** Exposes a clean RESTful endpoint (`GET /api/v1/signals`) for client applications (like the Nega-City Console) to fetch the latest signals.
- **Manual Trigger:** Exposes `POST /api/v1/ingest` for on-demand synchronous extraction.

### 4. Persistence Layer (PostgreSQL)
- **State Management:** Extracted JSON signals are parsed by Go and upserted into a containerized PostgreSQL 16 database.
- **Deduplication:** The `url` acts as a unique constraint. Existing signals are updated, preventing database bloat and ensuring idempotency.

## Tech Stack
- **Python 3.12+** (Data Collection & LLM Integration)
- **Google Generative AI SDK** (Gemini 2.5 Flash)
- **Go 1.22+** (Core Routing, Concurrency, API)
- **PostgreSQL 16** (Persistent Storage)
- **Docker & Docker Compose** (Infrastructure)

## Running the Engine

1. Start the PostgreSQL database:
   ```bash
   docker-compose up -d
   ```

2. Run the Go Orchestrator:
   ```bash
   go run main.go
   ```