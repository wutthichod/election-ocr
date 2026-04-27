# 🗳️ Thailand Election 2026 OCR Pipeline

Convert 319 scanned PDF election forms → Clean SQL database for analysis. Built for 2110446 Data Science & Data Engineering course.

## 🎯 What It Does

```
PDFs → Images → OCR → Structured Data → Validated → Database
319    638      API    JSON (285 stations) ✓        DuckDB
```

**Tech Stack:** Python · Async I/O · Vision AI (Typhoon/Claude/GPT-4o) · Pydantic · DuckDB

---

## 📐 Pipeline Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                    DATA TRANSFORMATION JOURNEY                       │
└─────────────────────────────────────────────────────────────────────┘

  📄 Raw PDFs                    🖼️  Images                  📝 Text
  ┌─────────┐                  ┌─────────┐               ┌─────────┐
  │ form.pdf│──── Stage 1 ────▶│page1.png│── Stage 2 ───▶│page1.md │
  │ form.pdf│   (rasterize)    │page2.png│    (OCR)      │page2.md │
  │ ...     │                  │  ...    │               │  ...    │
  └─────────┘                  └─────────┘               └─────────┘
       │                                                       │
       │ Stage 0                                               │
       │ (manifest)                                            ▼
       ▼                                                  ┌──────────┐
  ┌──────────┐                                            │  JSON    │
  │ Manifest │                                            │ Pydantic │
  │  + hash  │                                            │  Schema  │
  └──────────┘                                            └──────────┘
                                                                │
                                                                │ Stage 3
                                                                │ (extract)
                                                                ▼
  ┌──────────────┐                                       ┌──────────────┐
  │   DuckDB     │◀──── Stage 5 (load) ─── Stage 4 ─────│  Validated   │
  │  Database    │                       (validate)     │     Data     │
  │              │                                       │              │
  │ • stations   │                                       │ ✓ Thai words │
  │ • candidates │                                       │ ✓ Sum votes  │
  └──────────────┘                                       │ ✓ Arithmetic │
       ⭐ FINAL                                          └──────────────┘
```

### How Each Stage Works

```
STAGE 0: MANIFEST                    Why: Track all files, detect duplicates
─────────────────                    
  data/raw/*.pdf                     • SHA-256 hash each PDF
       │                              • Detect identical files
       ▼                              • Create master inventory
  manifest.parquet                    
                                     ⏱️  1 min · 💾 ~50KB

STAGE 1: RASTERIZE                   Why: OCR needs images, not PDFs
──────────────────                   
  manifest.parquet                   • Parallel processing (4 workers)
       │                              • 300 DPI for accuracy
       ▼                              • Content-addressed storage
  bronze/pages/                       • Idempotent (resumable)
  └── <hash>/page_NN.png             
                                     ⏱️  15 min · 💾 ~2GB

STAGE 2: OCR                         Why: Extract text from images
────────────                         
  pages.parquet + PNGs               • Async API calls (2-8 concurrent)
       │                              • Vision AI (Typhoon/Claude/GPT-4o)
       ▼                              • Cache results (rerun = instant)
  bronze/markdown/                    • Retry with exponential backoff
  └── <hash>/page_NN.md              
                                     ⏱️  25 min · 💾 ~50MB · 💰 $0.60-6

STAGE 3: EXTRACT                     Why: Convert text to structured data
────────────────                     
  markdown files                     • LLM-based extraction (instructor)
       │                              • Pydantic schema validation
       ▼                              • Group multi-page forms by station
  silver/extracted.parquet            • Type-safe (int, date, etc.)
                                     ⏱️  3 min · 💾 ~5MB

STAGE 4-5: VALIDATE & LOAD           Why: Ensure data integrity
──────────────────────────           
  extracted.parquet                  • Cross-check Thai words ↔ digits
       │                              • Verify: Σ(votes) = ballots_valid
       ▼                              • Check: 2.2.1+2.2.2+2.2.3 = 2.2
  gold/elections.duckdb               • Quarantine failures
       +                              • Normalize into 2 SQL tables
  quarantine/*.parquet               
                                     ⏱️  1 min · 💾 ~10MB
```

### Why This Architecture?

```
┌──────────────────────────────────────────────────────────────────┐
│  MEDALLION ARCHITECTURE (Bronze → Silver → Gold)                 │
├──────────────────────────────────────────────────────────────────┤
│                                                                   │
│   🥉 BRONZE         🥈 SILVER          🥇 GOLD                   │
│   (Raw)            (Cleaned)          (Curated)                  │
│                                                                   │
│   • PNG images     • Pydantic JSON    • DuckDB tables            │
│   • Markdown       • Type validated   • Domain validated         │
│   • Cached         • Schema enforced  • Query-ready              │
│                                                                   │
│   "What we got"    "What it means"    "What we trust"            │
└──────────────────────────────────────────────────────────────────┘
```

**Why split into stages?**
- ✓ **Idempotent:** Each stage can be rerun independently
- ✓ **Cacheable:** Failures don't lose work from earlier stages
- ✓ **Debuggable:** Inspect data at each layer
- ✓ **Scalable:** Same pattern for 100 or 100,000 PDFs

---

## ⚡ Quick Start

```bash
# 1. Setup (5 min)
git clone https://github.com/yourusername/election-ocr-pipeline.git
cd election-ocr-pipeline
conda create -n dsde python=3.10 -y && conda activate dsde
conda install -c conda-forge pandas pyarrow polars poppler -y
pip install pdf2image Pillow httpx aiohttp tenacity pydantic pydantic-settings \
    instructor openai pythainlp duckdb structlog typer 'tqdm[asyncio]'

# 2. Configure API key (get from https://opentyphoon.ai or https://console.anthropic.com)
cat > .env <<EOF
OCR_PROVIDER=typhoon
OCR_OCR_API_KEY=your-api-key-here
OCR_EXTRACTOR_API_KEY=your-api-key-here
OCR_CONCURRENCY=2
EOF

# 3. Organize PDFs
mkdir -p data/raw/5_18
# Put your PDFs in data/raw/5_18/*.pdf

# 4. Run pipeline (30-60 min total)
python scripts/run_stage.py build-manifest      # Stage 0: 1 min
python scripts/run_stage.py rasterize-pdfs      # Stage 1: 15 min
python scripts/run_stage.py ocr                 # Stage 2: 25 min
python scripts/run_stage.py extract-schemas     # Stage 3: 3 min
python scripts/run_stage.py load                # Stage 4-5: 1 min

# 5. Query your database
python -c "
import duckdb
con = duckdb.connect('data/gold/elections.duckdb')
con.sql('SELECT COUNT(*) FROM stations').show()
con.sql('SELECT party, SUM(votes) FROM candidate_votes GROUP BY party ORDER BY 2 DESC').show()
"
```

---

## 🏗️ Pipeline Stages Summary

| Stage | Input | Output | Key Tech | Time |
|-------|-------|--------|----------|------|
| **0: Manifest** | PDFs | `manifest.parquet` | SHA-256 hashing | 1m |
| **1: Rasterize** | PDFs | PNG images (300 DPI) | `pdf2image` + multiprocessing | 15m |
| **2: OCR** | Images | Markdown text | Async vision API | 25m |
| **3: Extract** | Markdown | JSON (Pydantic) | `instructor` + LLM | 3m |
| **4: Validate** | JSON | Validated JSON | Domain rules | 1m |
| **5: Load** | JSON | DuckDB tables | SQL normalization | 1m |

---

## 📊 Database Schema

```sql
-- stations: One row per polling station
CREATE TABLE stations (
    sha256 VARCHAR PRIMARY KEY,
    polling_station INT, tambon VARCHAR, amphoe VARCHAR, constituency INT,
    voters_registered INT, voters_present INT,
    ballots_valid INT, ballots_invalid INT, total_votes INT
);

-- candidate_votes: One row per candidate per station
CREATE TABLE candidate_votes (
    sha256 VARCHAR, candidate_number INT, candidate_name VARCHAR,
    party VARCHAR, votes INT,
    PRIMARY KEY (sha256, candidate_number)
);
```

**Example Queries:**

```sql
-- Turnout by district
SELECT amphoe, AVG(voters_present::FLOAT/voters_registered*100) as turnout
FROM stations GROUP BY amphoe ORDER BY turnout DESC;

-- Top parties
SELECT party, SUM(votes) FROM candidate_votes GROUP BY party ORDER BY 2 DESC;
```

---

## 🔧 Configuration

**Switch OCR provider** (in `.env`):

```bash
# Typhoon (Thai-specialized, cheap ~$0.60 total)
OCR_PROVIDER=typhoon
OCR_OCR_MODEL=typhoon-ocr-1-5

# Claude (most reliable)
OCR_PROVIDER=claude
OCR_OCR_MODEL=claude-3-5-sonnet-20241022

# GPT-4o (fast)
OCR_PROVIDER=openai
OCR_OCR_MODEL=gpt-4o
```

**Adjust performance:**
- `OCR_CONCURRENCY=2` (reduce if hitting rate limits)
- `dpi: int = 300` in `config.py` (lower = faster, less accurate)

---

## 🐛 Common Issues

| Error | Fix |
|-------|-----|
| `ModuleNotFoundError: election_ocr` | Run from project root: `cd election-ocr-pipeline` |
| `400 Bad Request` during OCR | Reduce `OCR_CONCURRENCY=1` or switch provider |
| `DecompressionBombWarning` | Add `Image.MAX_IMAGE_PIXELS = None` in `rasterize.py` |
| `page_num is float` | Add `pages["page_num"] = pages["page_num"].astype(int)` in `run_ocr.py` |

---

## 📁 Project Structure

```
election-ocr-pipeline/
├── .env                    # API keys (gitignored)
├── data/
│   ├── raw/5_18/*.pdf     # Input PDFs
│   ├── bronze/            # Images + markdown (cached)
│   ├── silver/            # Extracted JSON
│   ├── gold/
│   │   └── elections.duckdb  # ⭐ Final database
│   └── quarantine/        # Validation failures
├── src/election_ocr/      # Pipeline code
│   ├── config.py          # Settings (loads .env)
│   ├── manifest.py        # Stage 0
│   ├── rasterize.py       # Stage 1
│   ├── ocr_client.py      # Stage 2 client
│   ├── run_ocr.py         # Stage 2 orchestration
│   ├── extract.py         # Stage 3
│   ├── validate.py        # Stage 4
│   └── store.py           # Stage 5
└── scripts/run_stage.py   # CLI entrypoint
```
