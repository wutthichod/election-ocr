import asyncio, sys, glob
sys.path.insert(0, 'src')
from election_ocr.ocr_client import TyphoonOCRClient
from election_ocr.config import settings
from pathlib import Path

async def test():
    client = TyphoonOCRClient(
        base_url=settings.vllm_url, model=settings.ocr_model,
        api_key=settings.ocr_api_key, concurrency=1, timeout_s=300)
    img = Path(sorted(glob.glob('data/bronze/pages/*/*/page_01.png'))[0])
    print('image:', img)
    print('sending to:', settings.vllm_url)
    try:
        result = await client.ocr_image(img)
        print('OK, chars:', len(result))
        print(result[:500])
    except Exception as e:
        print('FAIL:', type(e).__name__)
        print('repr:', repr(e))
    await client.close()

asyncio.run(test())
