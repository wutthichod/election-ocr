# 1. Setup (5 min)

conda create -n dsde python=3.10 -y && conda activate dsde

conda install -c conda-forge pandas pyarrow polars poppler -y

pip install pdf2image Pillow httpx aiohttp tenacity pydantic pydantic-settings \
    instructor openai pythainlp duckdb structlog typer 'tqdm[asyncio]'


# 2. Configure API key

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
