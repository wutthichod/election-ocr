# src/election_ocr/validate.py
from dataclasses import dataclass
from .schemas.form_518 import Form518
from pythainlp.util import thai_digit_to_arabic_digit
import re

@dataclass
class ValidationResult:
    passed: bool
    issues: list[str]
    warnings: list[str]

def _thai_word_to_int(text: str) -> int | None:
    """Parse Thai number words like 'ห้าร้อยสามสิบแปด' → 538.
    Uses pythainlp when possible, falls back to None."""
    try:
        from pythainlp.util import thai_word_to_num
        return thai_word_to_num(text.strip())
    except Exception:
        return None

def validate_518(r: Form518) -> ValidationResult:
    issues, warnings = [], []

    # Cross-check 1: digit ↔ Thai word for every candidate
    for c in r.candidates:
        parsed = _thai_word_to_int(c.votes_thai_word)
        if parsed is None:
            warnings.append(f"C#{c.number}: unparseable Thai word '{c.votes_thai_word}'")
        elif parsed != c.votes_digit:
            issues.append(
                f"C#{c.number}: digit={c.votes_digit} vs Thai word={parsed}"
            )

    # Cross-check 2: sum of votes == ballots_valid
    summed = sum(c.votes_digit for c in r.candidates)
    if summed != r.ballots_valid:
        issues.append(f"Σ candidate votes ({summed}) ≠ ballots_valid ({r.ballots_valid})")
    if summed != r.total_votes:
        issues.append(f"Σ candidate votes ({summed}) ≠ total_votes ({r.total_votes})")

    # Cross-check 3: ballot subtotals
    subtotal = r.ballots_valid + r.ballots_invalid + r.ballots_no_vote
    if subtotal != r.ballots_used:
        issues.append(f"2.2.1+2.2.2+2.2.3 ({subtotal}) ≠ 2.2 ({r.ballots_used})")

    # Cross-check 4: allocated = used + remaining
    if r.ballots_allocated != r.ballots_used + r.ballots_remaining:
        issues.append(
            f"2.1 ({r.ballots_allocated}) ≠ 2.2 ({r.ballots_used}) + "
            f"2.3 ({r.ballots_remaining})"
        )

    # Sanity bounds
    if r.voters_present > r.voters_registered:
        issues.append("voters_present > voters_registered")
    if r.ballots_used > r.voters_present:
        issues.append("ballots_used > voters_present")

    # Soft warnings (not hard failures, but flag for review)
    turnout = r.voters_present / max(r.voters_registered, 1)
    if turnout > 0.95:
        warnings.append(f"Very high turnout: {turnout:.1%}")
    if turnout < 0.30:
        warnings.append(f"Very low turnout: {turnout:.1%}")
    invalid_rate = r.ballots_invalid / max(r.ballots_used, 1)
    if invalid_rate > 0.05:
        warnings.append(f"High invalid-ballot rate: {invalid_rate:.1%}")

    return ValidationResult(passed=len(issues) == 0, issues=issues, warnings=warnings)