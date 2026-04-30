# src/election_ocr/store.py
import asyncio
import duckdb
import pandas as pd
from pathlib import Path
from .config import settings
from .validate import validate_518, validate_partylist
from .schemas.form_518 import Form518
from .schemas.form_partylist import FormPartylist
from .logging import log

SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS constituency_stations (
    sha256 VARCHAR PRIMARY KEY,
    voting_type VARCHAR,
    station_id VARCHAR,
    tambon VARCHAR, amphoe VARCHAR, changwat VARCHAR,
    constituency INTEGER,
    polling_station INTEGER,
    election_date DATE,
    voters_registered INTEGER, voters_present INTEGER,
    ballots_allocated INTEGER, ballots_used INTEGER,
    ballots_valid INTEGER, ballots_invalid INTEGER,
    ballots_no_vote INTEGER, ballots_remaining INTEGER,
    total_votes INTEGER,
    validation_passed BOOLEAN,
    issues VARCHAR[], warnings VARCHAR[]
);

CREATE TABLE IF NOT EXISTS candidate_votes (
    sha256 VARCHAR,
    voting_type VARCHAR,
    candidate_number INTEGER,
    candidate_name VARCHAR,
    party VARCHAR,
    votes INTEGER,
    PRIMARY KEY (sha256, candidate_number)
);

CREATE TABLE IF NOT EXISTS partylist_stations (
    sha256 VARCHAR PRIMARY KEY,
    voting_type VARCHAR,
    station_id VARCHAR,
    tambon VARCHAR, amphoe VARCHAR, changwat VARCHAR,
    constituency INTEGER,
    polling_station INTEGER,
    election_date DATE,
    voters_registered INTEGER, voters_present INTEGER,
    ballots_allocated INTEGER, ballots_used INTEGER,
    ballots_valid INTEGER, ballots_invalid INTEGER,
    ballots_no_vote INTEGER, ballots_remaining INTEGER,
    total_votes INTEGER,
    validation_passed BOOLEAN,
    issues VARCHAR[], warnings VARCHAR[]
);

CREATE TABLE IF NOT EXISTS partylist_votes (
    sha256 VARCHAR,
    voting_type VARCHAR,
    party_number INTEGER,
    party_name VARCHAR,
    votes INTEGER,
    PRIMARY KEY (sha256, party_number)
);
"""

_VOTING_TYPE = {
    "5_18":           "election_day",
    "5_17":           "advance",
    "5_18_partylist": "election_day",
    "5_17_partylist": "advance",
}


def _station_row(obj, vr, voting_type: str) -> dict:
    return {
        "sha256": obj.source_sha256,
        "voting_type": voting_type,
        "station_id": None,
        "tambon": obj.tambon, "amphoe": obj.amphoe, "changwat": obj.changwat,
        "constituency": obj.constituency,
        "polling_station": obj.polling_station,
        "election_date": obj.election_date,
        "voters_registered": obj.voters_registered,
        "voters_present": obj.voters_present,
        "ballots_allocated": obj.ballots_allocated,
        "ballots_used": obj.ballots_used,
        "ballots_valid": obj.ballots_valid,
        "ballots_invalid": obj.ballots_invalid,
        "ballots_no_vote": obj.ballots_no_vote,
        "ballots_remaining": obj.ballots_remaining,
        "total_votes": obj.total_votes,
        "validation_passed": True,
        "issues": [], "warnings": vr.warnings,
    }


def _process_silver(silver: pd.DataFrame):
    """Validate and split silver records into good rows and quarantine rows."""
    constituency_station_rows, candidate_rows = [], []
    partylist_station_rows, partylist_vote_rows = [], []
    quarantine_rows = []

    for rec in silver.itertuples():
        form_type = rec.form_type
        voting_type = _VOTING_TYPE.get(form_type)
        sha = rec.sha256

        try:
            if form_type in ("5_18", "5_17"):
                obj = Form518(**rec.data)
                vr = validate_518(obj)
                if not vr.passed:
                    quarantine_rows.append({
                        "sha256": sha, "form_type": form_type,
                        "issues": vr.issues, "warnings": vr.warnings, "data": rec.data,
                    })
                    continue
                constituency_station_rows.append(_station_row(obj, vr, voting_type))
                for c in obj.candidates:
                    candidate_rows.append({
                        "sha256": sha, "voting_type": voting_type,
                        "candidate_number": c.number, "candidate_name": c.name,
                        "party": c.party, "votes": c.votes_digit,
                    })

            elif form_type in ("5_18_partylist", "5_17_partylist"):
                obj = FormPartylist(**rec.data)
                vr = validate_partylist(obj)
                if not vr.passed:
                    quarantine_rows.append({
                        "sha256": sha, "form_type": form_type,
                        "issues": vr.issues, "warnings": vr.warnings, "data": rec.data,
                    })
                    continue
                partylist_station_rows.append(_station_row(obj, vr, voting_type))
                for p in obj.parties:
                    partylist_vote_rows.append({
                        "sha256": sha, "voting_type": voting_type,
                        "party_number": p.number, "party_name": p.name, "votes": p.votes_digit,
                    })

        except Exception as e:
            quarantine_rows.append({
                "sha256": sha, "form_type": form_type,
                "issues": [str(e)[:300]], "warnings": [], "data": rec.data,
            })

    return (constituency_station_rows, candidate_rows,
            partylist_station_rows, partylist_vote_rows,
            quarantine_rows)


def load_to_duckdb():
    from .extract import re_extract_for_shas

    settings.gold_db.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(settings.gold_db))
    con.execute(SCHEMA_DDL)

    silver = pd.read_parquet(settings.silver_dir / "extracted.parquet")

    (cs_rows, cand_rows, pl_rows, plv_rows, quarantine) = _process_silver(silver)

    # Re-extract validation failures with improved schema validators as feedback
    if quarantine:
        log.info("re_extracting_failures", count=len(quarantine))
        pairs = [(r["sha256"], r["form_type"], r.get("issues", [])) for r in quarantine]
        new_recs = asyncio.run(re_extract_for_shas(pairs))

        recovered = [r for r in new_recs if r["status"] == "ok"]
        still_bad = [r for r in new_recs if r["status"] != "ok"]

        if recovered:
            recovered_df = pd.DataFrame(recovered)
            (cs2, cand2, pl2, plv2, q2) = _process_silver(recovered_df)
            cs_rows += cs2;  cand_rows += cand2
            pl_rows += pl2;  plv_rows += plv2
            quarantine = q2 + still_bad
            log.info("re_extract_recovered", recovered=len(recovered) - len(q2), still_failed=len(quarantine))

    def _insert(table: str, rows: list):
        df = pd.DataFrame(rows)
        con.register("_tmp", df)
        con.execute(f"INSERT OR REPLACE INTO {table} SELECT * FROM _tmp")
        con.unregister("_tmp")

    if cs_rows:
        _insert("constituency_stations", cs_rows)
    if cand_rows:
        _insert("candidate_votes", cand_rows)
    if pl_rows:
        _insert("partylist_stations", pl_rows)
    if plv_rows:
        _insert("partylist_votes", plv_rows)
    if quarantine:
        pd.DataFrame(quarantine).to_parquet(
            settings.quarantine_dir / "validation_failures.parquet", index=False
        )

    log.info(
        "duckdb_loaded",
        constituency_stations=len(cs_rows),
        candidate_rows=len(cand_rows),
        partylist_stations=len(pl_rows),
        partylist_vote_rows=len(plv_rows),
        quarantined=len(quarantine),
    )
    con.close()
