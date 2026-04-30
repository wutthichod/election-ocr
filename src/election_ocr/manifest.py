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

def _detect_form_types(pdf: Path, raw_dir: Path) -> list[str]:
    """Infer which form types are in a PDF from its directory path.

    Directory naming conventions:
      "5-18 เเละ 5-18(บช)"  → election-day constituency + party-list
      "5-17 และ 5-17(บช)"   → advance constituency + party-list
      "ในเขต"               → advance in-district (same 5-17 structure)
    """
    path_str = str(pdf.relative_to(raw_dir))
    if "5-18" in path_str:
        return ["5_18", "5_18_partylist"]
    if "5-17" in path_str or "ในเขต" in path_str:
        return ["5_17", "5_17_partylist"]
    # Fallback: assume election-day if directory name is unrecognised
    return ["5_18", "5_18_partylist"]

def build_manifest() -> pd.DataFrame:
    """Inventory every PDF under raw/. Each PDF is one polling station.
    form_types records which tally forms are inside (detected from directory name)."""
    rows = []
    for pdf in settings.raw_dir.rglob("*.pdf"):
        try:
            rows.append({
                "station_id": pdf.stem,
                "path": str(pdf),
                "size_bytes": pdf.stat().st_size,
                "sha256": sha256_file(pdf),
                "form_types": _detect_form_types(pdf, settings.raw_dir),
            })
        except Exception as e:
            log.error("manifest_failed", path=str(pdf), error=str(e))

    df = pd.DataFrame(rows)
    out = settings.data_root / "manifest.parquet"
    df.to_parquet(out, index=False)

    dupes = df[df.duplicated("sha256", keep=False)]
    tiny = df[df.size_bytes < 20_000]
    log.info("manifest_built",
             total=len(df),
             duplicates=len(dupes),
             suspicious_small=len(tiny),
             by_form=df.explode("form_types").groupby("form_types").size().to_dict())
    return df