# tests/test_validate.py
import pytest
from election_ocr.schemas.form_518 import Form518, Candidate
from election_ocr.validate import validate_518

def _make(**overrides):
    base = dict(
        source_sha256="a"*64, source_pages=[1, 2],
        polling_station=79, tambon="สบเมย", amphoe="แม่แจ่ม",
        changwat="ลำปาง", constituency=4, election_date="2026-02-08",
        voters_registered=538, voters_present=364,
        ballots_allocated=480, ballots_used=364,
        ballots_valid=332, ballots_invalid=3, ballots_no_vote=29,
        ballots_remaining=116, total_votes=332,
        candidates=[Candidate(number=1, name="A", party="P", votes_digit=332, votes_thai_word="สามร้อยสามสิบสอง")],
    )
    base.update(overrides)
    return Form518(**base)

def test_clean_form_passes():
    r = _make()
    assert validate_518(r).passed

def test_sum_mismatch_fails():
    r = _make(ballots_valid=999)
    result = validate_518(r)
    assert not result.passed
    assert any("ballots_valid" in i for i in result.issues)