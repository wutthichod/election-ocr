# scripts/run_stage.py
import sys
from pathlib import Path

# Add src/ to Python's search path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root / "src"))

import typer
import asyncio
from election_ocr.logging import setup_logging
from election_ocr import manifest, rasterize, run_ocr, extract, store

app = typer.Typer()

@app.command()
def build_manifest():
    setup_logging(); manifest.build_manifest()

@app.command()
def rasterize_pdfs(workers: int = 4):
    setup_logging(); rasterize.rasterize_all(workers=workers)

@app.command()
def ocr():
    setup_logging(); asyncio.run(run_ocr.ocr_batch())

@app.command()
def extract_schemas():
    setup_logging(); asyncio.run(extract.extract_all())

@app.command()
def load():
    setup_logging(); store.load_to_duckdb()

@app.command()
def all():
    """Run the full pipeline end to end."""
    setup_logging()
    manifest.build_manifest()
    rasterize.rasterize_all()
    asyncio.run(run_ocr.ocr_batch())
    asyncio.run(extract.extract_all())
    store.load_to_duckdb()

if __name__ == "__main__":
    app()