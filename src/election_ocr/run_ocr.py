# src/election_ocr/run_ocr.py
import asyncio
import pandas as pd
from pathlib import Path
from tqdm.asyncio import tqdm_asyncio
from .config import settings
from .ocr_client import TyphoonOCRClient
from .logging import log
import structlog

async def ocr_batch():
    pages = pd.read_parquet(settings.data_root / "pages.parquet")
    pages = pages[pages["image_path"].notna()].reset_index(drop=True)

    client = TyphoonOCRClient(
        base_url=settings.vllm_url,
        model=settings.ocr_model,
        concurrency=settings.ocr_concurrency,
    )

    out_base = settings.bronze_dir / "markdown"
    results = []

    async def process_row(row):
        bound_log = structlog.get_logger().bind(
            sha256=row.sha256[:12], page=row.page_num
        )
        image = Path(row.image_path)
        # Mirror rasterize layout for cache
        cache = out_base / row.sha256[:2] / row.sha256 / f"page_{row.page_num:02d}.md"
        try:
            md = await client.ocr_image(image, cache_path=cache)
            return {"sha256": row.sha256, "page_num": row.page_num,
                    "markdown_path": str(cache), "chars": len(md),
                    "status": "ok"}
        except Exception as e:
            bound_log.error("ocr_failed", error=str(e))
            return {"sha256": row.sha256, "page_num": row.page_num,
                    "status": "error", "error": str(e)[:200]}

    try:
        tasks = [process_row(r) for r in pages.itertuples()]
        results = await tqdm_asyncio.gather(*tasks, desc="ocr")
    finally:
        await client.close()

    df = pd.DataFrame(results)
    df.to_parquet(settings.data_root / "ocr_results.parquet", index=False)

    ok = (df.status == "ok").sum()
    log.info("ocr_batch_done", ok=int(ok), errors=len(df) - int(ok),
             total_chars=int(df.chars.sum() if "chars" in df else 0))

if __name__ == "__main__":
    asyncio.run(ocr_batch())