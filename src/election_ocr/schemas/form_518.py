# src/election_ocr/schemas/form_518.py
from pydantic import BaseModel, Field, model_validator
from datetime import date
from typing import Optional

class Candidate(BaseModel):
    number: int = Field(..., ge=1, le=30)
    name: str
    party: str
    votes_digit: int = Field(..., ge=0)
    votes_thai_word: str

class Form518(BaseModel):
    # Provenance
    source_sha256: str
    source_pages: list[int]

    # Location
    polling_station: int
    tambon: str
    amphoe: str
    changwat: str
    constituency: int

    # Timing
    election_date: date

    # Section 1
    voters_registered: int = Field(..., ge=0)
    voters_present: int = Field(..., ge=0)

    # Section 2
    ballots_allocated: int = Field(..., ge=0)
    ballots_used: int = Field(..., ge=0)
    ballots_valid: int = Field(..., ge=0)
    ballots_invalid: int = Field(..., ge=0)
    ballots_no_vote: int = Field(..., ge=0)
    ballots_remaining: int = Field(..., ge=0)

    # Section 3
    candidates: list[Candidate]
    total_votes: int

    # Extraction metadata
    extraction_confidence: Optional[float] = None
    extractor_model: Optional[str] = None

    @model_validator(mode="after")
    def _basic_sanity(self):
        # Only light checks here — heavy validation happens in validate.py
        # so we can still persist suspicious rows to quarantine
        return self