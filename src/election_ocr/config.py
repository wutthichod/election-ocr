# src/election_ocr/config.py
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="OCR_")

    # Paths
    data_root: Path = Path("data")
    raw_dir: Path = Path("data/raw")
    bronze_dir: Path = Path("data/bronze")
    silver_dir: Path = Path("data/silver")
    gold_db: Path = Path("data/gold/elections.duckdb")
    quarantine_dir: Path = Path("data/quarantine")

    # Rasterization
    dpi: int = 300
    image_format: str = "PNG"

    # OCR endpoint
    vllm_url: str = "http://localhost:8000/v1"
    ocr_model: str = "typhoon-ocr-1-5"
    ocr_concurrency: int = 8          # tune per GPU: T4=4, A100=12
    ocr_timeout_s: int = 120
    ocr_max_retries: int = 3

    # Schema extraction
    extractor_base_url: str = "https://api.opentyphoon.ai/v1"
    extractor_model: str = "typhoon-v2.1-12b-instruct"
    extractor_concurrency: int = 6
    extractor_api_key: str = ""

settings = Settings()