# src/election_ocr/store.py
import asyncio
import csv
import duckdb
import pandas as pd
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
        "tambon": obj.tambon, "amphoe": obj.amphoe, "changwat": "ลำปาง",
        "constituency": 1,
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
        "validation_passed": vr.passed,
        "issues": vr.issues, "warnings": vr.warnings,
    }


def _process_silver(silver: pd.DataFrame, strict: bool = False):
    """
    Process silver records.

    strict=True  → quarantine validation failures (for correction loop)
    strict=False → include all records regardless of validation status

    Returns (cs_rows, cand_rows, pl_rows, plv_rows, quarantine).
    quarantine is always empty when strict=False.
    """
    cs_rows, cand_rows, pl_rows, plv_rows, quarantine = [], [], [], [], []

    for rec in silver.itertuples():
        form_type = rec.form_type
        voting_type = _VOTING_TYPE.get(form_type)
        sha = rec.sha256

        try:
            if form_type in ("5_18", "5_17"):
                obj = Form518(**rec.data)
                vr = validate_518(obj)
                if strict and not vr.passed:
                    quarantine.append({
                        "sha256": sha, "form_type": form_type,
                        "issues": vr.issues, "data": rec.data,
                    })
                    continue
                cs_rows.append(_station_row(obj, vr, voting_type))
                for c in obj.candidates:
                    cand_rows.append({
                        "sha256": sha, "voting_type": voting_type,
                        "candidate_number": c.number, "candidate_name": c.name,
                        "party": c.party, "votes": c.votes_digit,
                    })

            elif form_type in ("5_18_partylist", "5_17_partylist"):
                obj = FormPartylist(**rec.data)
                vr = validate_partylist(obj)
                if strict and not vr.passed:
                    quarantine.append({
                        "sha256": sha, "form_type": form_type,
                        "issues": vr.issues, "data": rec.data,
                    })
                    continue
                pl_rows.append(_station_row(obj, vr, voting_type))
                for p in obj.parties:
                    plv_rows.append({
                        "sha256": sha, "voting_type": voting_type,
                        "party_number": p.number, "party_name": p.name, "votes": p.votes_digit,
                    })

        except Exception as e:
            log.warning("process_silver_error", sha=sha[:12], form_type=form_type, error=str(e)[:200])

    return cs_rows, cand_rows, pl_rows, plv_rows, quarantine


def _normalize_names(con: duckdb.DuckDBPyConnection) -> None:
    """
    Canonicalize candidate and party names across polling stations.

    For each candidate_number (or party_number) within a constituency,
    pick the most frequently occurring name transcription as the canonical one
    and update all rows to use it.
    """
    # --- candidate names ---
    cand_df = con.execute("SELECT * FROM candidate_votes").df()
    if not cand_df.empty:
        st_df = con.execute("SELECT sha256, constituency FROM constituency_stations").df()
        merged = cand_df.merge(st_df, on="sha256")
        counts = (
            merged.groupby(["constituency", "candidate_number", "candidate_name"])
            .size().reset_index(name="cnt")
        )
        canonical = (
            counts.sort_values("cnt", ascending=False)
            .drop_duplicates(["constituency", "candidate_number"])
            [["constituency", "candidate_number", "candidate_name"]]
            .rename(columns={"candidate_name": "canonical_name"})
        )
        merged = merged.merge(canonical, on=["constituency", "candidate_number"])
        merged["candidate_name"] = merged["canonical_name"]
        merged = merged.drop(columns=["constituency", "canonical_name"])

        # Also canonicalize party name per (constituency, candidate_number)
        merged2 = merged.merge(st_df, on="sha256")
        party_counts = (
            merged2.groupby(["constituency", "candidate_number", "party"])
            .size().reset_index(name="cnt")
        )
        canon_party = (
            party_counts.sort_values("cnt", ascending=False)
            .drop_duplicates(["constituency", "candidate_number"])
            [["constituency", "candidate_number", "party"]]
            .rename(columns={"party": "canonical_party"})
        )
        merged2 = merged2.merge(canon_party, on=["constituency", "candidate_number"])
        merged2["party"] = merged2["canonical_party"]
        merged = merged2.drop(columns=["constituency", "canonical_party"])

        con.execute("DELETE FROM candidate_votes")
        con.register("_tmp", merged)
        con.execute("INSERT INTO candidate_votes SELECT * FROM _tmp")
        con.unregister("_tmp")
        log.info("names_normalized", type="candidate", unique=len(canonical))

    # --- party names ---
    party_df = con.execute("SELECT * FROM partylist_votes").df()
    if not party_df.empty:
        st_df = con.execute("SELECT sha256, constituency FROM partylist_stations").df()
        merged = party_df.merge(st_df, on="sha256")
        counts = (
            merged.groupby(["constituency", "party_number", "party_name"])
            .size().reset_index(name="cnt")
        )
        canonical = (
            counts.sort_values("cnt", ascending=False)
            .drop_duplicates(["constituency", "party_number"])
            [["constituency", "party_number", "party_name"]]
            .rename(columns={"party_name": "canonical_name"})
        )
        merged = merged.merge(canonical, on=["constituency", "party_number"])
        merged["party_name"] = merged["canonical_name"]
        merged = merged.drop(columns=["constituency", "canonical_name"])
        con.execute("DELETE FROM partylist_votes")
        con.register("_tmp", merged)
        con.execute("INSERT INTO partylist_votes SELECT * FROM _tmp")
        con.unregister("_tmp")
        log.info("names_normalized", type="party", unique=len(canonical))


def load_to_duckdb():
    from .correct import correction_loop

    settings.gold_db.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(settings.gold_db))
    try:
        con.execute(SCHEMA_DDL)

        silver = pd.read_parquet(settings.silver_dir / "extracted.parquet")

        # Pass 1: strict — separate clean records from failures
        cs_rows, cand_rows, pl_rows, plv_rows, quarantine = _process_silver(silver, strict=True)
        log.info("initial_validation", clean=len(cs_rows) + len(pl_rows), quarantined=len(quarantine))

        # Pass 2: correction loop — re-extract (LLM) and re-OCR until fixed
        if quarantine:
            try:
                recovered, still_bad = asyncio.run(correction_loop(quarantine))
            except Exception as e:
                log.warning("correction_loop_failed", error=str(e)[:200])
                recovered, still_bad = [], quarantine
            if recovered:
                recovered_df = pd.DataFrame(recovered)
                cs2, cand2, pl2, plv2, _ = _process_silver(recovered_df, strict=False)
                cs_rows += cs2; cand_rows += cand2; pl_rows += pl2; plv_rows += plv2
            # Pass 3: bypass remaining failures — include with validation_passed=False
            if still_bad:
                log.info("bypass_remaining", count=len(still_bad))
                bypass_df = pd.DataFrame([
                    {"sha256": r["sha256"], "form_type": r["form_type"], "data": r["data"]}
                    for r in still_bad if "data" in r
                ])
                if not bypass_df.empty:
                    cs2, cand2, pl2, plv2, _ = _process_silver(bypass_df, strict=False)
                    cs_rows += cs2; cand_rows += cand2; pl_rows += pl2; plv_rows += plv2

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

        # Post-process: canonicalize names and parties across stations
        _normalize_names(con)

        log.info(
            "duckdb_loaded",
            constituency_stations=len(cs_rows),
            candidate_rows=len(cand_rows),
            partylist_stations=len(pl_rows),
            partylist_vote_rows=len(plv_rows),
        )
    finally:
        con.close()


def _write_csv(path, cols: list[str], rows: list[tuple]) -> None:
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(rows)


def _party_column_label(number: int, name: str) -> str:
    party_name = "" if pd.isna(name) else str(name).strip()
    if not party_name:
        party_name = "ไม่ทราบชื่อ"
    return f"{int(number)}. {party_name}"


def _export_tambon_party_csv(
    con: duckdb.DuckDBPyConnection,
    out_dir,
    *,
    station_table: str,
    vote_table: str,
    station_alias: str,
    vote_alias: str,
    number_col: str,
    name_col: str,
    filename: str,
) -> None:
    ballot_df = con.execute(f"""
        SELECT
            'ลำปาง'                     AS จังหวัด,
            {station_alias}.constituency AS เขต,
            {station_alias}.amphoe       AS อำเภอ,
            {station_alias}.tambon       AS ตำบล,
            COUNT(*)                     AS จำนวนหน่วย,
            SUM({station_alias}.voters_registered) AS ผู้มีสิทธิ,
            SUM({station_alias}.voters_present)    AS มาใช้สิทธิ,
            SUM({station_alias}.ballots_used)      AS บัตรใช้ไป,
            SUM({station_alias}.ballots_valid)     AS บัตรดี,
            SUM({station_alias}.ballots_invalid)   AS บัตรเสีย,
            SUM({station_alias}.ballots_no_vote)   AS บัตรไม่เลือก
        FROM {station_table} {station_alias}
        WHERE {station_alias}.constituency = 1
          AND EXISTS (
              SELECT 1
              FROM {vote_table} ballot_votes
              WHERE ballot_votes.sha256 = {station_alias}.sha256
                AND ballot_votes.votes <= {station_alias}.ballots_valid
                AND COALESCE(TRIM(ballot_votes.{name_col}), '') <> ''
          )
        GROUP BY {station_alias}.constituency, {station_alias}.amphoe, {station_alias}.tambon
    """).df()

    vote_df = con.execute(f"""
        SELECT
            {station_alias}.constituency AS เขต,
            {station_alias}.amphoe       AS อำเภอ,
            {station_alias}.tambon       AS ตำบล,
            {vote_alias}.{number_col}    AS หมายเลข,
            {vote_alias}.{name_col}      AS พรรค,
            SUM({vote_alias}.votes)      AS คะแนน
        FROM {vote_table} {vote_alias}
        JOIN {station_table} {station_alias} ON {vote_alias}.sha256 = {station_alias}.sha256
        WHERE {station_alias}.constituency = 1
          AND {vote_alias}.votes <= {station_alias}.ballots_valid
          AND COALESCE(TRIM({vote_alias}.{name_col}), '') <> ''
        GROUP BY
            {station_alias}.constituency,
            {station_alias}.amphoe,
            {station_alias}.tambon,
            {vote_alias}.{number_col},
            {vote_alias}.{name_col}
    """).df()

    party_cols: list[str] = []
    if vote_df.empty:
        out_df = ballot_df.copy()
    else:
        vote_df["party_col"] = vote_df.apply(
            lambda row: _party_column_label(row["หมายเลข"], row["พรรค"]),
            axis=1,
        )
        party_cols = (
            vote_df[["หมายเลข", "party_col"]]
            .drop_duplicates()
            .sort_values(["หมายเลข", "party_col"])
            ["party_col"]
            .tolist()
        )
        pivot = (
            vote_df.pivot_table(
                index=["เขต", "อำเภอ", "ตำบล"],
                columns="party_col",
                values="คะแนน",
                aggfunc="sum",
                fill_value=0,
            )
            .reset_index()
        )
        for col in party_cols:
            if col not in pivot.columns:
                pivot[col] = 0
        pivot = pivot[["เขต", "อำเภอ", "ตำบล", *party_cols]]
        out_df = ballot_df.merge(pivot, on=["เขต", "อำเภอ", "ตำบล"], how="left")

    numeric_cols = [
        "เขต",
        "จำนวนหน่วย",
        "ผู้มีสิทธิ",
        "มาใช้สิทธิ",
        "บัตรใช้ไป",
        "บัตรดี",
        "บัตรเสีย",
        "บัตรไม่เลือก",
        *party_cols,
    ]
    for col in numeric_cols:
        if col in out_df.columns:
            out_df[col] = out_df[col].fillna(0).astype(int)

    ordered_cols = [
        "จังหวัด",
        "เขต",
        "อำเภอ",
        "ตำบล",
        "จำนวนหน่วย",
        "ผู้มีสิทธิ",
        "มาใช้สิทธิ",
        "บัตรใช้ไป",
        "บัตรดี",
        "บัตรเสีย",
        "บัตรไม่เลือก",
        *party_cols,
    ]
    out_df = out_df[ordered_cols].sort_values(["เขต", "อำเภอ", "ตำบล"])
    out_df.to_csv(out_dir / filename, index=False, encoding="utf-8-sig")
    log.info("exported", file=filename, rows=len(out_df), party_columns=len(party_cols))


def export_csvs():
    """Export constituency-wide and tambon-level election summary CSVs."""
    out_dir = settings.data_root / "output"
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(settings.gold_db), read_only=True)

    # constituency.csv — aggregate by (constituency, candidate_number) after name normalization
    # Filter cv.votes <= cs.ballots_valid to exclude OCR-misread vote counts
    con.execute("""
        SELECT
            'ลำปาง'             AS จังหวัด,
            cs.constituency      AS เขต,
            cv.candidate_number  AS หมายเลข,
            cv.candidate_name    AS ชื่อผู้สมัคร,
            cv.party             AS พรรค,
            SUM(cv.votes)        AS คะแนน
        FROM candidate_votes cv
        JOIN constituency_stations cs ON cv.sha256 = cs.sha256
        WHERE cs.constituency = 1
          AND cv.votes <= cs.ballots_valid
        GROUP BY cs.constituency, cv.candidate_number, cv.candidate_name, cv.party
        HAVING SUM(cv.votes) > 0 AND cv.candidate_name IS NOT NULL AND cv.candidate_name <> ''
        ORDER BY cv.candidate_number
    """)
    rows = con.fetchall()
    cols = ["จังหวัด", "เขต", "หมายเลข", "ชื่อผู้สมัคร", "พรรค", "คะแนน"]
    _write_csv(out_dir / "constituency.csv", cols, rows)
    log.info("exported", file="constituency.csv", rows=len(rows))

    # party_list.csv
    # Filter pv.votes <= ps.ballots_valid to exclude OCR-misread vote counts
    con.execute("""
        SELECT
            'ลำปาง'            AS จังหวัด,
            ps.constituency     AS เขต,
            pv.party_number     AS หมายเลข,
            pv.party_name       AS พรรค,
            SUM(pv.votes)       AS คะแนน
        FROM partylist_votes pv
        JOIN partylist_stations ps ON pv.sha256 = ps.sha256
        WHERE ps.constituency = 1
          AND pv.votes <= ps.ballots_valid
        GROUP BY ps.constituency, pv.party_number, pv.party_name
        ORDER BY pv.party_number
    """)
    rows = con.fetchall()
    cols = ["จังหวัด", "เขต", "หมายเลข", "พรรค", "คะแนน"]
    _write_csv(out_dir / "party_list.csv", cols, rows)
    log.info("exported", file="party_list.csv", rows=len(rows))

    _export_tambon_party_csv(
        con,
        out_dir,
        station_table="constituency_stations",
        vote_table="candidate_votes",
        station_alias="cs",
        vote_alias="cv",
        number_col="candidate_number",
        name_col="party",
        filename="constituency_tambon.csv",
    )

    _export_tambon_party_csv(
        con,
        out_dir,
        station_table="partylist_stations",
        vote_table="partylist_votes",
        station_alias="ps",
        vote_alias="pv",
        number_col="party_number",
        name_col="party_name",
        filename="party_list_tambon.csv",
    )

    # summary_winners.csv
    con.execute("""
        WITH agg AS (
            SELECT
                constituency,
                SUM(voters_registered) AS ผู้มีสิทธิ,
                SUM(voters_present)    AS มาใช้สิทธิ,
                SUM(ballots_valid)     AS คะแนนดี
            FROM constituency_stations
            WHERE constituency = 1
            GROUP BY constituency
        ),
        cand_agg AS (
            SELECT
                cs.constituency,
                cv.candidate_number,
                cv.candidate_name,
                cv.party,
                SUM(cv.votes) AS total_votes
            FROM candidate_votes cv
            JOIN constituency_stations cs ON cv.sha256 = cs.sha256
            WHERE cs.constituency = 1
              AND cv.votes <= cs.ballots_valid
            GROUP BY cs.constituency, cv.candidate_number, cv.candidate_name, cv.party
        ),
        winners AS (
            SELECT DISTINCT ON (constituency)
                constituency, candidate_number, candidate_name, party, total_votes
            FROM cand_agg
            ORDER BY constituency, total_votes DESC
        )
        SELECT
            'ลำปาง'          AS จังหวัด,
            w.constituency    AS เขต,
            w.candidate_name  AS ผู้ชนะ,
            w.party           AS พรรค,
            w.total_votes     AS คะแนน,
            a.คะแนนดี,
            a.ผู้มีสิทธิ,
            a.มาใช้สิทธิ
        FROM winners w
        JOIN agg a ON w.constituency = a.constituency
    """)
    rows = con.fetchall()
    cols = ["จังหวัด", "เขต", "ผู้ชนะ", "พรรค", "คะแนน", "คะแนนดี", "ผู้มีสิทธิ", "มาใช้สิทธิ"]
    _write_csv(out_dir / "summary_winners.csv", cols, rows)
    log.info("exported", file="summary_winners.csv", rows=len(rows))

    con.close()
