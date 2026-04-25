# src/election_ocr/store.py
import duckdb
import pandas as pd
from pathlib import Path
from .config import settings
from .validate import validate_518
from .schemas.form_518 import Form518
from .logging import log

SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS stations (
    sha256 VARCHAR PRIMARY KEY,
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
    candidate_number INTEGER,
    candidate_name VARCHAR,
    party VARCHAR,
    votes INTEGER,
    PRIMARY KEY (sha256, candidate_number)
);
"""

def load_to_duckdb():
    settings.gold_db.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(settings.gold_db))
    con.execute(SCHEMA_DDL)

    silver = pd.read_parquet(settings.silver_dir / "extracted.parquet")
    station_rows, candidate_rows, quarantine_rows = [], [], []

    for rec in silver.itertuples():
        obj = Form518(**rec.data)
        vr = validate_518(obj)

        if not vr.passed:
            quarantine_rows.append({
                "sha256": obj.source_sha256,
                "issues": vr.issues,
                "warnings": vr.warnings,
                "data": rec.data,
            })
            continue  # skip loading; route to manual review

        station_rows.append({
            "sha256": obj.source_sha256,
            "station_id": None,  # join-filled later
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
        })
        for c in obj.candidates:
            candidate_rows.append({
                "sha256": obj.source_sha256,
                "candidate_number": c.number,
                "candidate_name": c.name,
                "party": c.party,
                "votes": c.votes_digit,
            })

    if station_rows:
        con.execute("INSERT OR REPLACE INTO stations SELECT * FROM station_df",
                    {"station_df": pd.DataFrame(station_rows)})
    if candidate_rows:
        con.execute("INSERT OR REPLACE INTO candidate_votes SELECT * FROM cand_df",
                    {"cand_df": pd.DataFrame(candidate_rows)})
    if quarantine_rows:
        pd.DataFrame(quarantine_rows).to_parquet(
            settings.quarantine_dir / "validation_failures.parquet", index=False
        )

    log.info("duckdb_loaded",
             stations=len(station_rows),
             candidate_rows=len(candidate_rows),
             quarantined=len(quarantine_rows))
    con.close()