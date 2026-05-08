# src/election_ocr/validate.py
from dataclasses import dataclass
from .schemas.form_518 import Form518
from .schemas.form_partylist import FormPartylist
from pythainlp.util import thai_digit_to_arabic_digit
import re

@dataclass
class ValidationResult:
    passed: bool
    issues: list[str]
    warnings: list[str]

def _thai_word_to_int(text: str) -> int | None:
    """Parse Thai number words like 'ห้าร้อยสามสิบแปด' → 538."""
    try:
        from pythainlp.util import thai_word_to_num
        return thai_word_to_num(text.strip())
    except Exception:
        return None

def _ballot_checks(r, issues: list[str], warnings: list[str], total_votes: int) -> None:
    """Shared ballot integrity checks for both form types."""
    if r.constituency != 1:
        issues.append(f"constituency is {r.constituency}, must be 1 (ลำปาง เขต 1)")

    subtotal = r.ballots_valid + r.ballots_invalid + r.ballots_no_vote
    if abs(subtotal - r.ballots_used) > 2:
        issues.append(f"2.2.1+2.2.2+2.2.3 ({subtotal}) ≠ 2.2 ({r.ballots_used})")
    elif subtotal != r.ballots_used:
        warnings.append(f"2.2.1+2.2.2+2.2.3 ({subtotal}) ≠ 2.2 ({r.ballots_used}) [within ±2 tolerance]")

    if r.ballots_allocated != r.ballots_used + r.ballots_remaining:
        msg = (
            f"2.1 ({r.ballots_allocated}) ≠ 2.2 ({r.ballots_used}) + "
            f"2.3 ({r.ballots_remaining})"
        )
        if r.ballots_allocated < r.ballots_used + r.ballots_remaining:
            warnings.append(msg + " (allocated < used+remaining — expected for advance-vote counting centres)")
        else:
            issues.append(msg)

    if r.voters_present > r.voters_registered:
        warnings.append("voters_present > voters_registered (expected for advance-vote counting centres)")
    if r.ballots_used > r.voters_present:
        warnings.append("ballots_used > voters_present (expected for advance-vote counting centres)")

    turnout = r.voters_present / max(r.voters_registered, 1)
    if turnout > 0.95:
        warnings.append(f"Very high turnout: {turnout:.1%}")
    if turnout < 0.30:
        warnings.append(f"Very low turnout: {turnout:.1%}")
    invalid_rate = r.ballots_invalid / max(r.ballots_used, 1)
    if invalid_rate > 0.05:
        warnings.append(f"High invalid-ballot rate: {invalid_rate:.1%}")


def validate_518(r: Form518) -> ValidationResult:
    issues, warnings = [], []

    # Cross-check 1: digit ↔ Thai word; individual vote sanity
    for c in r.candidates:
        parsed = _thai_word_to_int(c.votes_thai_word)
        if parsed is None:
            warnings.append(f"C#{c.number}: unparseable Thai word '{c.votes_thai_word}'")
        elif parsed != c.votes_digit:
            issues.append(f"C#{c.number}: digit={c.votes_digit} vs Thai word={parsed}")
        # A single candidate cannot receive more votes than registered voters (unless advance centre)
        if c.votes_digit > r.voters_registered:
            msg = (
                f"C#{c.number}: votes ({c.votes_digit}) > voters_registered ({r.voters_registered})"
                " — OCR likely misread digit count, re-OCR required"
            )
            if r.voters_present > r.voters_registered:
                warnings.append(msg + " (advance-vote centre — verify)")
            else:
                issues.append(msg)

    # Cross-check 2: sum of votes == ballots_valid / total_votes (±2 tolerance for transcription noise)
    summed = sum(c.votes_digit for c in r.candidates)
    if abs(summed - r.ballots_valid) > 2:
        issues.append(f"Σ candidate votes ({summed}) ≠ ballots_valid ({r.ballots_valid})")
    elif summed != r.ballots_valid:
        warnings.append(f"Σ candidate votes ({summed}) ≠ ballots_valid ({r.ballots_valid}) [within ±2 tolerance]")
    if abs(summed - r.total_votes) > 2:
        issues.append(f"Σ candidate votes ({summed}) ≠ total_votes ({r.total_votes})")
    elif summed != r.total_votes:
        warnings.append(f"Σ candidate votes ({summed}) ≠ total_votes ({r.total_votes}) [within ±2 tolerance]")

    _ballot_checks(r, issues, warnings, summed)
    return ValidationResult(passed=len(issues) == 0, issues=issues, warnings=warnings)


def validate_partylist(r: FormPartylist) -> ValidationResult:
    issues, warnings = [], []

    # Cross-check 1: digit ↔ Thai word; individual vote sanity
    for p in r.parties:
        parsed = _thai_word_to_int(p.votes_thai_word)
        if parsed is None:
            warnings.append(f"Party#{p.number}: unparseable Thai word '{p.votes_thai_word}'")
        elif parsed != p.votes_digit:
            issues.append(f"Party#{p.number}: digit={p.votes_digit} vs Thai word={parsed}")
        # A single party cannot receive more votes than registered voters (unless advance centre)
        if p.votes_digit > r.voters_registered:
            msg = (
                f"Party#{p.number}: votes ({p.votes_digit}) > voters_registered ({r.voters_registered})"
                " — OCR likely misread digit count, re-OCR required"
            )
            if r.voters_present > r.voters_registered:
                warnings.append(msg + " (advance-vote centre — verify)")
            else:
                issues.append(msg)

    # Cross-check 2: sum of votes == ballots_valid / total_votes (±2 tolerance for transcription noise)
    summed = sum(p.votes_digit for p in r.parties)
    if abs(summed - r.ballots_valid) > 2:
        issues.append(f"Σ party votes ({summed}) ≠ ballots_valid ({r.ballots_valid})")
    elif summed != r.ballots_valid:
        warnings.append(f"Σ party votes ({summed}) ≠ ballots_valid ({r.ballots_valid}) [within ±2 tolerance]")
    if abs(summed - r.total_votes) > 2:
        issues.append(f"Σ party votes ({summed}) ≠ total_votes ({r.total_votes})")
    elif summed != r.total_votes:
        warnings.append(f"Σ party votes ({summed}) ≠ total_votes ({r.total_votes}) [within ±2 tolerance]")

    _ballot_checks(r, issues, warnings, summed)
    return ValidationResult(passed=len(issues) == 0, issues=issues, warnings=warnings)
