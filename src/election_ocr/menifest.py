# src/election_ocr/manifest.py
import hashlib
import pandas as pd
from pathlib import Path
from .config import settings
from .logging import log

def sha256_file(path: Path, chunk: int = 2**20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while block := f.read(chunk):
            h.update(block)
    return h.hexdigest()

def build_manifest() -> pd.DataFrame:
    """Inventory every PDF under raw/. Uses content hash for idempotency."""
    rows = []
    for form_dir in settings.raw_dir.iterdir():
        if not form_dir.is_dir():
            continue
        for pdf in form_dir.rglob("*.pdf"):
            try:
                rows.append({
                    "form_type": form_dir.name,
                    "station_id": pdf.stem,
                    "path": str(pdf),
                    "size_bytes": pdf.stat().st_size,
                    "sha256": sha256_file(pdf),
                })
            except Exception as e:
                log.error("manifest_failed", path=str(pdf), error=str(e))

    df = pd.DataFrame(rows)
    out = settings.data_root / "manifest.parquet"
    df.to_parquet(out, index=False)

    # Health checks
    dupes = df[df.duplicated("sha256", keep=False)]
    tiny = df[df.size_bytes < 20_000]
    log.info("manifest_built",
             total=len(df),
             duplicates=len(dupes),
             suspicious_small=len(tiny),
             by_form=df.groupby("form_type").size().to_dict())
    return df