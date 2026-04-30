import asyncio
import pandas as pd
from pathlib import Path
from tqdm.asyncio import tqdm
from .config import settings
from .ocr_client import TyphoonOCRClient
from .logging import log

async def ocr_batch():
    """Run OCR on all pages."""
    log.info("starting OCR batch")
    
    pages = pd.read_parquet(settings.data_root / "pages.parquet")
    pages = pages[pages["image_path"].notna()].reset_index(drop=True)
    pages["page_num"] = pages["page_num"].astype(int)
    
    client = TyphoonOCRClient(
        base_url=settings.vllm_url,
        model=settings.ocr_model,
        api_key=settings.ocr_api_key,  # ADDED THIS
        concurrency=settings.ocr_concurrency,
        timeout_s=settings.ocr_timeout_s,
    )
    
    out_base = settings.bronze_dir / "markdown"
    
    async def process_row(row):
        image = Path(row.image_path)
        cache = out_base / row.sha256[:2] / row.sha256 / f"page_{row.page_num:02d}.md"
        
        try:
            md = await client.ocr_image(image, cache_path=cache)
            return {
                "sha256": row.sha256,
                "page_num": row.page_num,
                "markdown_path": str(cache),
                "chars": len(md),
                "status": "ok"
            }
        except Exception as e:
            log.error("ocr failed", sha=row.sha256[:12], page=row.page_num,
                      exc_type=type(e).__name__, error=repr(e))
            return {
                "sha256": row.sha256,
                "page_num": row.page_num,
                "status": "error",
                "error": repr(e)[:200]
            }
    
    try:
        tasks = [process_row(r) for r in pages.itertuples()]
        results = list(await tqdm.gather(*tasks))
    finally:
        await client.close()
    
    df = pd.DataFrame(results)
    out_path = settings.data_root / "ocr_results.parquet"
    df.to_parquet(out_path, index=False)
    
    ok = (df.status == "ok").sum()
    log.info("OCR complete", ok=int(ok), errors=len(df) - int(ok), output=str(out_path))
    
    return df