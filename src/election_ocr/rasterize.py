# src/election_ocr/rasterize.py
import pandas as pd
from pathlib import Path
from pdf2image import convert_from_path
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
from .config import settings
from .logging import log

def rasterize_one(pdf_path: str, sha256: str, dpi: int) -> list[dict]:
    """Content-addressed: pages stored under sha256 prefix so rerunning
    is free for unchanged PDFs."""
    pdf = Path(pdf_path)
    out_dir = settings.bronze_dir / "pages" / sha256[:2] / sha256
    out_dir.mkdir(parents=True, exist_ok=True)

    existing = sorted(out_dir.glob("page_*.png"))
    if existing:
        return [{"sha256": sha256, "page_num": int(p.stem.split("_")[1]),
                 "image_path": str(p), "cached": True} for p in existing]

    try:
        images = convert_from_path(pdf, dpi=dpi, thread_count=1)
        rows = []
        for i, img in enumerate(images, 1):
            p = out_dir / f"page_{i:02d}.png"
            img.save(p, "PNG", optimize=True)
            rows.append({"sha256": sha256, "page_num": i,
                         "image_path": str(p), "cached": False})
        return rows
    except Exception as e:
        return [{"sha256": sha256, "error": str(e)}]

def rasterize_all(workers: int = 4) -> pd.DataFrame:
    manifest = pd.read_parquet(settings.data_root / "manifest.parquet")
    all_pages = []

    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(rasterize_one, r.path, r.sha256, settings.dpi): r
            for r in manifest.itertuples()
        }
        for fut in tqdm(as_completed(futures), total=len(futures), desc="rasterize"):
            all_pages.extend(fut.result())

    pages_df = pd.DataFrame(all_pages)
    # Join back to manifest for station_id / form_type
    pages_df = pages_df.merge(manifest[["sha256", "station_id", "form_type"]], on="sha256")
    pages_df.to_parquet(settings.data_root / "pages.parquet", index=False)

    n_cached = pages_df["cached"].sum() if "cached" in pages_df else 0
    log.info("rasterize_done", total_pages=len(pages_df), cached=int(n_cached))
    return pages_df