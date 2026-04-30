# src/election_ocr/schemas/form_partylist.py
from pydantic import BaseModel, Field
from datetime import date
from typing import Optional


class PartyVote(BaseModel):
    number: int = Field(..., ge=1)
    name: str
    votes_digit: int = Field(..., ge=0)
    votes_thai_word: str


class FormPartylist(BaseModel):
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

    # Section 3 — one row per party (not individual candidate)
    parties: list[PartyVote]
    total_votes: int

    # Extraction metadata
    extraction_confidence: Optional[float] = None
    extractor_model: Optional[str] = None
