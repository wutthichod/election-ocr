from __future__ import annotations

import colorsys
import hashlib
import json
import math
import re
from collections import defaultdict
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen

import altair as alt
import pandas as pd
import pydeck as pdk
import streamlit as st


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "data" / "output"
VIZ_DIR = Path(__file__).resolve().parent
CACHE_DIR = VIZ_DIR / "cache"
BOUNDARY_CACHE = CACHE_DIR / "lampang_tambon_boundaries.geojson"
OVERRIDE_PATH = VIZ_DIR / "tambon_name_overrides.csv"
STYLE_PATH = VIZ_DIR / "styles.css"
BOUNDARY_ENDPOINT = (
    "https://services1.arcgis.com/jSaRWj2TDlcN1zOC/arcgis/rest/services/"
    "Thailand_Subdistrict_Boundaries_%28%E0%B8%82%E0%B9%89%E0%B8%AD%E0%B8%A1%E0%B8%B9%E0%B8%A5"
    "%E0%B8%82%E0%B8%AD%E0%B8%9A%E0%B9%80%E0%B8%82%E0%B8%95%E0%B8%95%E0%B8%B3%E0%B8%9A%E0%B8%A5"
    "%E0%B8%9B%E0%B8%A3%E0%B8%B0%E0%B9%80%E0%B8%97%E0%B8%A8%E0%B9%84%E0%B8%97%E0%B8%A2%29/FeatureServer/1/query"
)
PROVINCE_NAME = "ลำปาง"
UNKNOWN_LABEL = "ไม่ระบุ"
FIXED_COLUMNS = {
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
}
DATASET_CONFIG = {
    "constituency": {
        "label": "สส.เขต",
        "label_long": "สส.เขต",
        "hero_title": "แผนที่พรรคชนะรายตำบล: สส.เขต",
        "hero_desc": "จับคู่ผลคะแนนจาก <code>constituency_tambon.csv</code> กับขอบเขตตำบลจริง แล้วระบายสีตามพรรคที่ได้คะแนนสูงสุดในแต่ละตำบล",
        "tambon_csv": "constituency_tambon.csv",
        "download_name": "constituency_tambon_winners_lampang.csv",
        "review_label": "คะแนนรวมผู้สมัครในแต่ละพรรค",
    },
    "party_list": {
        "label": "บัญชีรายชื่อ",
        "label_long": "บัญชีรายชื่อ",
        "hero_title": "แผนที่พรรคชนะรายตำบล: บัญชีรายชื่อ",
        "hero_desc": "จับคู่ผลคะแนนจาก <code>party_list_tambon.csv</code> กับขอบเขตตำบลจริง แล้วระบายสีตามพรรคที่ได้คะแนนบัญชีรายชื่อสูงสุดในแต่ละตำบล",
        "tambon_csv": "party_list_tambon.csv",
        "download_name": "party_list_tambon_winners_lampang.csv",
        "review_label": "คะแนนรวมรายพรรค",
    },
}
LAST_ELECTION_FILES = {
    "constituency": OUTPUT_DIR / "last_election_constituency_scores.csv",
    "party_list": OUTPUT_DIR / "last_election_party_list_scores.csv",
}
PARTY_ALIAS_MAP = {
    "ก้าวไกล": "ประชาชน",
    "ประชาชน": "ประชาชน",
}
FOCUS_AMPHOE = ("ห้างฉัตร", "เมืองลำปาง")
FOCUS_EXCLUDED_MUEANG_TAMBONS = (
    "บ้านแลง",
    "บ้านเสด็จ",
    "พิชัย",
)
FOCUS_EXCLUDED_ANY_AMPHOE_TAMBONS = (
    "ทุ่งกว๋าว",
    "บ้านขอ",
    "เมืองปาน",
    "แจ้ซ้อน",
    "หัวเมือง",
)

PARTY_COLOR_OVERRIDES = {
    "ประชาชน": "#f58220",
    "เพื่อไทย": "#ed1c24",
    "ภูมิใจไทย": "#1d3f95",
    "ประชาธิปัตย์": "#43b3e8",
    "รวมไทยสร้างชาติ": "#1e4f9c",
    "พลังประชารัฐ": "#2e3192",
    "ประชาชาติ": "#c9a227",
    "ไทยสร้างไทย": "#7c43a8",
    "เป็นธรรม": "#2b6cb0",
}
DEFAULT_FILL = [181, 188, 202, 110]
DEFAULT_LINE = [255, 255, 255, 180]
HIGHLIGHT_LINE = [27, 33, 48, 235]
MAP_PANEL_SIZE = 680
HISTOGRAM_PANEL_HEIGHT = 720
INITIAL_MAP_ZOOM = 7.6 + math.log2(3)


@dataclass(frozen=True)
class OfficialTambon:
    amphoe: str
    tambon: str
    amphoe_norm: str
    tambon_norm: str
    key: str


@dataclass(frozen=True)
class MatchResult:
    official_key: str | None
    official_amphoe: str | None
    official_tambon: str | None
    score: float
    method: str
    best_guess_key: str | None = None
    best_guess_amphoe: str | None = None
    best_guess_tambon: str | None = None


def normalize_place_name(value: Any) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return ""
    text = text.replace("อำเภอ", "").replace("ตำบล", "").replace("จังหวัด", "")
    text = text.replace("เมืองฯ", "เมืองลำปาง")
    text = re.sub(r"\s+", "", text)
    text = re.sub(r"[^0-9A-Za-z\u0E00-\u0E7F]", "", text)
    return text


def normalize_amphoe_name(value: Any) -> str:
    text = normalize_place_name(value)
    if "เมือง" in text:
        return normalize_place_name("เมืองลำปาง")
    return text


def display_name(value: Any) -> str:
    norm = normalize_place_name(value)
    return str(value).strip() if norm else UNKNOWN_LABEL


def similarity(left: str, right: str) -> float:
    if not left or not right:
        return 0.0
    if left == right:
        return 1.0
    return SequenceMatcher(None, left, right).ratio()


def party_parts(label: str) -> tuple[str, str]:
    if ". " in label:
        number, party = label.split(". ", 1)
        return number.strip(), party.strip()
    return label.strip(), label.strip()


def normalize_party_name(value: Any) -> str:
    text = str(value).strip()
    if text.startswith("พรรค"):
        text = text[4:].strip()
    return normalize_place_name(text)


def display_party_name(value: Any) -> str:
    text = str(value).strip()
    if not text:
        return ""
    return text if text.startswith("พรรค") else f"พรรค{text}"


def canonical_party_name(value: Any) -> str:
    key = normalize_party_name(value)
    return PARTY_ALIAS_MAP.get(key, key)


def to_int(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def color_to_hex(color: list[int]) -> str:
    return f"#{color[0]:02x}{color[1]:02x}{color[2]:02x}"


def hex_to_rgba(color: str, alpha: int = 210) -> list[int]:
    value = color.lstrip("#")
    if len(value) != 6:
        return DEFAULT_FILL.copy()
    return [int(value[0:2], 16), int(value[2:4], 16), int(value[4:6], 16), alpha]


def fallback_party_color(party_name: str) -> list[int]:
    key = normalize_party_name(party_name) or normalize_place_name(party_name)
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()
    hue = int(digest[:2], 16) / 255
    saturation = 0.52 + ((int(digest[2:4], 16) / 255) * 0.16)
    lightness = 0.46 + ((int(digest[4:6], 16) / 255) * 0.12)
    red, green, blue = colorsys.hls_to_rgb(hue, lightness, saturation)
    return [int(red * 255), int(green * 255), int(blue * 255), 210]


def is_focus_location(amphoe: Any, tambon: Any) -> bool:
    amphoe_norm = normalize_amphoe_name(amphoe)
    tambon_norm = normalize_place_name(tambon)
    hang_chat_norm = normalize_amphoe_name("ห้างฉัตร")
    mueang_norm = normalize_amphoe_name("เมืองลำปาง")
    excluded = {normalize_place_name(name) for name in FOCUS_EXCLUDED_MUEANG_TAMBONS}
    excluded_any_amphoe = {normalize_place_name(name) for name in FOCUS_EXCLUDED_ANY_AMPHOE_TAMBONS}

    if tambon_norm in excluded_any_amphoe:
        return False

    if amphoe_norm == hang_chat_norm:
        return True
    if amphoe_norm == mueang_norm and tambon_norm and tambon_norm not in excluded:
        return True
    return False


def inject_styles() -> None:
    if STYLE_PATH.exists():
        css = STYLE_PATH.read_text(encoding="utf-8")
        st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)


@st.cache_data(show_spinner=False)
def load_tambon_dataset(dataset_key: str) -> tuple[pd.DataFrame, list[str]]:
    path = OUTPUT_DIR / DATASET_CONFIG[dataset_key]["tambon_csv"]
    df = pd.read_csv(path)
    vote_cols = [col for col in df.columns if col not in FIXED_COLUMNS]

    numeric_cols = [
        "จำนวนหน่วย",
        "ผู้มีสิทธิ",
        "มาใช้สิทธิ",
        "บัตรใช้ไป",
        "บัตรดี",
        "บัตรเสีย",
        "บัตรไม่เลือก",
        *vote_cols,
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["raw_amphoe"] = df["อำเภอ"].apply(display_name)
    df["raw_tambon"] = df["ตำบล"].apply(display_name)
    df["raw_amphoe_norm"] = df["อำเภอ"].apply(normalize_amphoe_name)
    df["raw_tambon_norm"] = df["ตำบล"].apply(normalize_place_name)
    df["raw_valid_votes"] = df[vote_cols].sum(axis=1).astype(int)
    df["turnout_pct"] = (
        (df["มาใช้สิทธิ"] / df["ผู้มีสิทธิ"].replace(0, pd.NA)).fillna(0) * 100
    ).round(1)
    return df, vote_cols


def build_boundary_query_url() -> str:
    params = {
        "where": f"NAME1='{PROVINCE_NAME}'",
        "outFields": "ADMIN_ID3,NAME1,NAME2,NAME3,POPULATION,HOUSE",
        "returnGeometry": "true",
        "outSR": 4326,
        "f": "geojson",
    }
    return f"{BOUNDARY_ENDPOINT}?{urlencode(params)}"


@st.cache_data(show_spinner=False)
def fetch_lampang_boundaries(force_refresh: bool = False) -> dict[str, Any]:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    if BOUNDARY_CACHE.exists() and not force_refresh:
        return json.loads(BOUNDARY_CACHE.read_text(encoding="utf-8"))

    url = build_boundary_query_url()
    with urlopen(url, timeout=60) as response:
        payload = json.loads(response.read().decode("utf-8"))

    BOUNDARY_CACHE.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    return payload


def official_records_from_geojson(geojson: dict[str, Any]) -> list[OfficialTambon]:
    records: dict[str, OfficialTambon] = {}
    for feature in geojson.get("features", []):
        props = feature.get("properties", {})
        amphoe = str(props.get("NAME2", "")).strip()
        tambon = str(props.get("NAME3", "")).strip()
        amphoe_norm = normalize_amphoe_name(amphoe)
        tambon_norm = normalize_place_name(tambon)
        if not amphoe_norm or not tambon_norm:
            continue
        key = f"{amphoe_norm}|{tambon_norm}"
        records.setdefault(
            key,
            OfficialTambon(
                amphoe=amphoe,
                tambon=tambon,
                amphoe_norm=amphoe_norm,
                tambon_norm=tambon_norm,
                key=key,
            ),
        )
    return sorted(records.values(), key=lambda item: (item.amphoe, item.tambon))


def load_override_map() -> dict[tuple[str, str], tuple[str, str]]:
    if not OVERRIDE_PATH.exists():
        return {}
    raw = pd.read_csv(OVERRIDE_PATH).fillna("")
    required = {"raw_amphoe", "raw_tambon", "official_amphoe", "official_tambon"}
    if not required.issubset(raw.columns):
        return {}

    mapping: dict[tuple[str, str], tuple[str, str]] = {}
    for row in raw.itertuples():
        raw_key = (
            normalize_amphoe_name(row.raw_amphoe),
            normalize_place_name(row.raw_tambon),
        )
        official_value = (
            str(row.official_amphoe).strip(),
            str(row.official_tambon).strip(),
        )
        if raw_key[0] or raw_key[1]:
            mapping[raw_key] = official_value
    return mapping


def match_raw_row(
    raw_amphoe: str,
    raw_tambon: str,
    officials: list[OfficialTambon],
    official_lookup: dict[str, OfficialTambon],
    amphoe_groups: dict[str, list[OfficialTambon]],
    override_map: dict[tuple[str, str], tuple[str, str]],
    threshold: float,
) -> MatchResult:
    raw_amp_norm = normalize_amphoe_name(raw_amphoe)
    raw_tam_norm = normalize_place_name(raw_tambon)
    if not raw_amp_norm and not raw_tam_norm:
        return MatchResult(None, None, None, 0.0, "missing")

    override = override_map.get((raw_amp_norm, raw_tam_norm))
    if override:
        amp_norm = normalize_amphoe_name(override[0])
        tam_norm = normalize_place_name(override[1])
        key = f"{amp_norm}|{tam_norm}"
        official = official_lookup.get(key)
        if official:
            return MatchResult(
                official.key,
                official.amphoe,
                official.tambon,
                1.0,
                "override",
            )

    exact_key = f"{raw_amp_norm}|{raw_tam_norm}"
    if exact_key in official_lookup:
        official = official_lookup[exact_key]
        return MatchResult(
            official.key,
            official.amphoe,
            official.tambon,
            1.0,
            "exact",
        )

    candidate_pool = officials
    amphoe_score = 0.0
    best_amphoe_norm = ""
    if raw_amp_norm:
        for amphoe_norm in amphoe_groups:
            score = similarity(raw_amp_norm, amphoe_norm)
            if score > amphoe_score:
                amphoe_score = score
                best_amphoe_norm = amphoe_norm
        if best_amphoe_norm and amphoe_score >= 0.45:
            candidate_pool = amphoe_groups[best_amphoe_norm]

    best_feature: OfficialTambon | None = None
    best_score = -1.0
    for official in candidate_pool:
        tambon_score = similarity(raw_tam_norm, official.tambon_norm)
        amp_score = similarity(raw_amp_norm, official.amphoe_norm) if raw_amp_norm else 0.0
        if raw_amp_norm:
            score = (tambon_score * 0.62) + (amp_score * 0.38)
        else:
            score = tambon_score
        if raw_tam_norm and official.tambon_norm.startswith(raw_tam_norm):
            score += 0.02
        if raw_tam_norm and raw_tam_norm.startswith(official.tambon_norm):
            score += 0.02
        if official is best_feature:
            score += 0.0
        if score > best_score:
            best_score = score
            best_feature = official

    if best_feature is None:
        return MatchResult(None, None, None, 0.0, "unmatched")

    if best_score >= threshold:
        return MatchResult(
            best_feature.key,
            best_feature.amphoe,
            best_feature.tambon,
            round(best_score, 3),
            "fuzzy",
            best_feature.key,
            best_feature.amphoe,
            best_feature.tambon,
        )

    return MatchResult(
        None,
        None,
        None,
        round(best_score, 3),
        "review",
        best_feature.key,
        best_feature.amphoe,
        best_feature.tambon,
    )


def compute_winner_columns(df: pd.DataFrame, vote_cols: list[str]) -> pd.DataFrame:
    if df.empty:
        df = df.copy()
        for col in [
            "winner_label",
            "winner_number",
            "winner_party",
            "winner_votes",
            "runner_up_label",
            "runner_up_number",
            "runner_up_party",
            "runner_up_votes",
            "margin_votes",
            "margin_pct_valid",
        ]:
            df[col] = []
        return df

    values = df[vote_cols].to_numpy(dtype=float)
    winner_idx = values.argmax(axis=1)
    top_values = values.max(axis=1)

    runner_up_values: list[int] = []
    winner_labels: list[str] = []
    runner_labels: list[str] = []
    winner_numbers: list[str] = []
    runner_numbers: list[str] = []
    winner_parties: list[str] = []
    runner_parties: list[str] = []
    margins: list[int] = []

    for row_idx, winner_col_idx in enumerate(winner_idx):
        ordered = values[row_idx].argsort()[::-1]
        winner_label = vote_cols[winner_col_idx]
        runner_col_idx = ordered[1] if len(ordered) > 1 else winner_col_idx
        runner_label = vote_cols[runner_col_idx]
        winner_vote = int(top_values[row_idx])
        runner_vote = int(values[row_idx][runner_col_idx])
        winner_no, winner_party = party_parts(winner_label)
        runner_no, runner_party = party_parts(runner_label)

        winner_labels.append(winner_label)
        runner_labels.append(runner_label)
        winner_numbers.append(winner_no)
        runner_numbers.append(runner_no)
        winner_parties.append(winner_party)
        runner_parties.append(runner_party)
        runner_up_values.append(runner_vote)
        margins.append(winner_vote - runner_vote)

    df = df.copy()
    df["winner_label"] = winner_labels
    df["winner_number"] = winner_numbers
    df["winner_party"] = winner_parties
    df["winner_votes"] = top_values.astype(int)
    df["runner_up_label"] = runner_labels
    df["runner_up_number"] = runner_numbers
    df["runner_up_party"] = runner_parties
    df["runner_up_votes"] = runner_up_values
    df["margin_votes"] = margins
    df["margin_pct_valid"] = (
        (df["margin_votes"] / df["บัตรดี"].replace(0, pd.NA)).fillna(0) * 100
    ).round(1)
    return df


def build_party_colors(party_names: list[str]) -> dict[str, list[int]]:
    color_map: dict[str, list[int]] = {}
    for party in sorted({str(name).strip() for name in party_names if str(name).strip()}):
        override = PARTY_COLOR_OVERRIDES.get(normalize_party_name(party))
        color_map[party] = hex_to_rgba(override) if override else fallback_party_color(party)
    return color_map


def geojson_bounds(geojson: dict[str, Any]) -> tuple[float, float, float, float]:
    xs: list[float] = []
    ys: list[float] = []

    def visit(coords: Any) -> None:
        if not coords:
            return
        if isinstance(coords[0], (int, float)):
            xs.append(float(coords[0]))
            ys.append(float(coords[1]))
            return
        for child in coords:
            visit(child)

    for feature in geojson.get("features", []):
        geometry = feature.get("geometry") or {}
        visit(geometry.get("coordinates"))

    if not xs or not ys:
        return 99.5, 18.0, 99.5, 18.0
    return min(xs), min(ys), max(xs), max(ys)


def attach_results_to_geojson(
    geojson: dict[str, Any],
    tambon_results: pd.DataFrame,
    party_colors: dict[str, list[int]],
) -> dict[str, Any]:
    results_by_key = {
        f"{normalize_amphoe_name(row['official_amphoe'])}|{normalize_place_name(row['official_tambon'])}": row
        for row in tambon_results.to_dict("records")
    }

    features: list[dict[str, Any]] = []
    for feature in geojson.get("features", []):
        props = dict(feature.get("properties", {}))
        amphoe = str(props.get("NAME2", "")).strip()
        tambon = str(props.get("NAME3", "")).strip()
        key = f"{normalize_amphoe_name(amphoe)}|{normalize_place_name(tambon)}"
        result = results_by_key.get(key)

        fill = DEFAULT_FILL
        line = DEFAULT_LINE
        has_result = False
        winner_party = "ไม่มีข้อมูล"
        winner_votes = 0
        margin_votes = 0
        turnout_pct = 0.0
        matched_rows = 0
        matched_score = 0.0
        runner_up_party = "ไม่มีข้อมูล"
        ballots_valid = 0
        if result is not None:
            fill = party_colors.get(result["winner_party"], DEFAULT_FILL)
            has_result = True
            winner_party = result["winner_party"]
            winner_votes = to_int(result["winner_votes"])
            margin_votes = to_int(result["margin_votes"])
            turnout_pct = to_float(result["turnout_pct"])
            matched_rows = to_int(result["source_rows"])
            matched_score = to_float(result["avg_match_score"])
            runner_up_party = result["runner_up_party"]
            ballots_valid = to_int(result["บัตรดี"])
            if matched_score >= 0.95:
                line = HIGHLIGHT_LINE

        props.update(
            {
                "province_name": str(props.get("NAME1", PROVINCE_NAME)),
                "amphoe_name": amphoe,
                "tambon_name": tambon,
                "has_result": int(has_result),
                "winner_party": winner_party,
                "winner_votes": winner_votes,
                "runner_up_party": runner_up_party,
                "margin_votes": margin_votes,
                "turnout_pct": turnout_pct,
                "matched_rows": matched_rows,
                "matched_score": round(matched_score, 3),
                "ballots_valid": ballots_valid,
                "fill_r": fill[0],
                "fill_g": fill[1],
                "fill_b": fill[2],
                "fill_a": fill[3],
                "line_r": line[0],
                "line_g": line[1],
                "line_b": line[2],
                "line_a": line[3],
            }
        )
        features.append(
            {
                "type": "Feature",
                "properties": props,
                "geometry": feature.get("geometry"),
            }
        )

    return {"type": "FeatureCollection", "features": features}


def build_map(
    geojson: dict[str, Any],
    center_lon: float,
    center_lat: float,
    zoom: float,
) -> pdk.Deck:
    layer = pdk.Layer(
        "GeoJsonLayer",
        id="winner-map",
        data=geojson,
        pickable=False,
        auto_highlight=False,
        stroked=True,
        filled=True,
        extruded=False,
        wireframe=False,
        get_fill_color="[properties.fill_r, properties.fill_g, properties.fill_b, properties.fill_a]",
        get_line_color="[properties.line_r, properties.line_g, properties.line_b, properties.line_a]",
        line_width_min_pixels=1,
    )

    return pdk.Deck(
        layers=[layer],
        views=[pdk.View(type="MapView", controller=False)],
        map_style=None,
        initial_view_state=pdk.ViewState(
            latitude=center_lat,
            longitude=center_lon,
            zoom=zoom,
            pitch=0,
        ),
    )


def aggregate_matched_rows(matched_rows: pd.DataFrame, vote_cols: list[str]) -> pd.DataFrame:
    numeric_cols = [
        "จำนวนหน่วย",
        "ผู้มีสิทธิ",
        "มาใช้สิทธิ",
        "บัตรใช้ไป",
        "บัตรดี",
        "บัตรเสีย",
        "บัตรไม่เลือก",
        "raw_valid_votes",
        *vote_cols,
    ]
    if matched_rows.empty:
        columns = [
            "official_key",
            "official_amphoe",
            "official_tambon",
            *numeric_cols,
            "source_rows",
            "avg_match_score",
            "strongest_method",
            "turnout_pct",
        ]
        return compute_winner_columns(pd.DataFrame(columns=columns), vote_cols)

    agg = (
        matched_rows.groupby(["official_key", "official_amphoe", "official_tambon"], as_index=False)[numeric_cols]
        .sum()
        .merge(
            matched_rows.groupby("official_key", as_index=False).agg(
                source_rows=("official_key", "size"),
                avg_match_score=("match_score", "mean"),
                strongest_method=("match_method", lambda values: sorted(set(values))[0]),
            ),
            on="official_key",
            how="left",
        )
    )
    agg["turnout_pct"] = (
        (agg["มาใช้สิทธิ"] / agg["ผู้มีสิทธิ"].replace(0, pd.NA)).fillna(0) * 100
    ).round(1)
    agg = compute_winner_columns(agg, vote_cols)
    return agg.sort_values(["winner_votes", "margin_votes"], ascending=[False, False]).reset_index(drop=True)


def filter_focus_geojson(geojson: dict[str, Any]) -> dict[str, Any]:
    features = []
    for feature in geojson.get("features", []):
        props = feature.get("properties", {})
        amphoe = props.get("amphoe_name", props.get("NAME2", ""))
        tambon = props.get("tambon_name", props.get("NAME3", ""))
        if is_focus_location(amphoe, tambon):
            features.append(feature)
    return {"type": "FeatureCollection", "features": features}


def build_vote_breakdown(rows: pd.DataFrame, vote_cols: list[str]) -> pd.DataFrame:
    if rows.empty:
        return pd.DataFrame(columns=["party_number", "party_name", "party_display", "votes", "label"])

    totals = rows[vote_cols].sum(axis=0)
    records = []
    for label, votes in totals.items():
        votes_int = to_int(votes)
        if votes_int <= 0:
            continue
        party_number, party_name = party_parts(label)
        records.append(
            {
                "party_number": party_number,
                "party_name": party_name,
                "party_display": display_party_name(party_name),
                "votes": votes_int,
                "label": label,
            }
        )
    return pd.DataFrame(records).sort_values(["votes", "party_number"], ascending=[False, True]).reset_index(drop=True)


def chart_height_for_rows(row_count: int) -> int:
    return HISTOGRAM_PANEL_HEIGHT


def comparison_chart_height(row_count: int, *, dataset_key: str) -> int:
    if dataset_key == "party_list":
        return min(1500, max(420, row_count * 34))
    return min(900, max(260, row_count * 34))


def render_vote_histogram(
    vote_breakdown: pd.DataFrame,
    title: str,
    party_colors: dict[str, list[int]] | None = None,
    fallback_color: str = "#376ff2",
    height: int | None = None,
) -> None:
    st.markdown(f"### {title}")
    if vote_breakdown.empty:
        st.info("ไม่มีข้อมูลคะแนนสำหรับกราฟนี้")
        return

    chart_df = vote_breakdown.copy()
    colors = party_colors or {}
    chart_df["bar_color"] = chart_df["party_name"].map(
        lambda name: color_to_hex(colors.get(name, [])) if name in colors else fallback_color
    )

    chart_height = height or chart_height_for_rows(len(chart_df))
    chart = (
        alt.Chart(chart_df)
        .mark_bar(cornerRadiusTopRight=5, cornerRadiusBottomRight=5)
        .encode(
            x=alt.X("votes:Q", title="คะแนนรวม"),
            y=alt.Y("party_display:N", sort="-x", title=None, axis=alt.Axis(labelLimit=260)),
            color=alt.Color("bar_color:N", scale=None, legend=None),
            tooltip=[
                alt.Tooltip("party_number:N", title="หมายเลข"),
                alt.Tooltip("party_display:N", title="พรรค"),
                alt.Tooltip("votes:Q", title="คะแนนรวม", format=","),
            ],
        )
        .properties(height=chart_height, background="rgba(0,0,0,0)")
        .configure_view(strokeOpacity=0)
        .configure_axis(
            labelColor="#eef4ff",
            titleColor="#eef4ff",
            domainColor="rgba(159, 194, 255, 0.35)",
            gridColor="rgba(159, 194, 255, 0.14)",
            tickColor="rgba(159, 194, 255, 0.35)",
        )
    )
    st.altair_chart(chart, use_container_width=True)


def load_last_election_scores(dataset_key: str) -> pd.DataFrame:
    path = LAST_ELECTION_FILES[dataset_key]
    if not path.exists():
        return pd.DataFrame(columns=["party_key", "party_display", "votes", "pct"])

    raw = pd.read_csv(path)
    if dataset_key == "constituency":
        raw = raw[raw["ประเภทข้อมูล"] == "ผู้สมัคร"].copy()
        party_col = "พรรค"
        votes_col = "คะแนน"
        pct_col = "คะแนนร้อยละ"
    else:
        party_col = "พรรค"
        votes_col = "คะแนน"
        pct_col = "คะแนนร้อยละ"

    if raw.empty:
        return pd.DataFrame(columns=["party_key", "party_display", "votes", "pct"])

    df = pd.DataFrame(
        {
            "party_key": raw[party_col].map(canonical_party_name),
            "votes": pd.to_numeric(raw[votes_col], errors="coerce").fillna(0),
            "pct": pd.to_numeric(raw[pct_col], errors="coerce").fillna(0.0),
        }
    )
    summary = df.groupby("party_key", as_index=False).agg(
        votes=("votes", "sum"),
        pct=("pct", "sum"),
    )
    summary["party_display"] = summary["party_key"].map(display_party_name)
    return summary


def load_current_election_scores(dataset_key: str) -> pd.DataFrame:
    source_df, vote_cols = load_tambon_dataset(dataset_key)
    breakdown = build_vote_breakdown(source_df, vote_cols)
    if breakdown.empty:
        return pd.DataFrame(columns=["party_key", "party_display", "votes", "pct"])

    total_votes = breakdown["votes"].sum()
    breakdown["party_key"] = breakdown["party_name"].map(canonical_party_name)
    summary = breakdown.groupby("party_key", as_index=False).agg(
        votes=("votes", "sum"),
    )
    summary["pct"] = ((summary["votes"] / total_votes) * 100).fillna(0).round(2)
    summary["party_display"] = summary["party_key"].map(display_party_name)
    return summary


def build_year_comparison(dataset_key: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    last_df = load_last_election_scores(dataset_key)
    current_df = load_current_election_scores(dataset_key)
    if last_df.empty or current_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    overlap = sorted(set(last_df["party_key"]) & set(current_df["party_key"]))
    if not overlap:
        return pd.DataFrame(), pd.DataFrame()

    last_overlap = last_df[last_df["party_key"].isin(overlap)].copy()
    current_overlap = current_df[current_df["party_key"].isin(overlap)].copy()

    combined = pd.concat(
        [
            last_overlap.assign(year_label="ครั้งก่อน"),
            current_overlap.assign(year_label="ปีนี้"),
        ],
        ignore_index=True,
    )
    combined["party_display"] = combined["party_key"].map(display_party_name)

    ranking = (
        combined.groupby("party_key", as_index=False)
        .agg(
            max_pct=("pct", "max"),
            current_pct=("pct", lambda values: values.iloc[-1] if len(values) else 0),
        )
        .sort_values(["current_pct", "max_pct"], ascending=[False, False])
    )
    party_order = ranking["party_key"].tolist()
    order_lookup = {party_key: idx for idx, party_key in enumerate(party_order)}
    combined["sort_order"] = combined["party_key"].map(order_lookup)
    combined = combined.sort_values(["sort_order", "year_label"]).reset_index(drop=True)

    comparison_table = (
        last_overlap[["party_key", "party_display", "votes", "pct"]]
        .rename(columns={"votes": "votes_last", "pct": "pct_last"})
        .merge(
            current_overlap[["party_key", "votes", "pct"]].rename(
                columns={"votes": "votes_current", "pct": "pct_current"}
            ),
            on="party_key",
            how="inner",
        )
    )
    comparison_table["pct_change"] = (comparison_table["pct_current"] - comparison_table["pct_last"]).round(2)
    comparison_table["vote_change"] = (comparison_table["votes_current"] - comparison_table["votes_last"]).astype(int)
    comparison_table["sort_order"] = comparison_table["party_key"].map(order_lookup)
    comparison_table = comparison_table.sort_values("sort_order").drop(columns=["sort_order", "party_key"])
    comparison_table = comparison_table.rename(
        columns={
            "party_display": "พรรค",
            "votes_last": "คะแนนครั้งก่อน",
            "votes_current": "คะแนนปีนี้",
            "vote_change": "คะแนนเปลี่ยน",
            "pct_last": "ร้อยละครั้งก่อน",
            "pct_current": "ร้อยละปีนี้",
            "pct_change": "ร้อยละเปลี่ยน",
        }
    )[
        [
            "พรรค",
            "คะแนนครั้งก่อน",
            "คะแนนปีนี้",
            "คะแนนเปลี่ยน",
            "ร้อยละครั้งก่อน",
            "ร้อยละปีนี้",
            "ร้อยละเปลี่ยน",
        ]
    ].reset_index(drop=True)

    return combined, comparison_table


def render_year_comparison_chart(comparison_df: pd.DataFrame, title: str, *, dataset_key: str) -> None:
    st.markdown(f"#### {title}")
    if comparison_df.empty:
        st.info("ยังไม่มีข้อมูลเปรียบเทียบสำหรับชุดนี้")
        return

    party_labels = comparison_df["party_display"].drop_duplicates().tolist()
    party_colors = build_party_colors(party_labels)
    chart_df = comparison_df.copy()
    chart_df["bar_color"] = chart_df["party_display"].map(
        lambda name: color_to_hex(party_colors.get(name, DEFAULT_FILL))
    )
    chart_df["year_opacity"] = chart_df["year_label"].map({"ครั้งก่อน": 0.45, "ปีนี้": 0.95}).fillna(0.9)
    party_order = chart_df.sort_values("sort_order")["party_display"].drop_duplicates().tolist()
    bar_size = 10 if dataset_key == "party_list" else 14
    y_padding_inner = 0.42 if dataset_key == "party_list" else 0.28
    y_padding_outer = 0.22 if dataset_key == "party_list" else 0.16

    chart = (
        alt.Chart(chart_df)
        .mark_bar(cornerRadiusEnd=5, size=bar_size)
        .encode(
            x=alt.X("pct:Q", title="คะแนนร้อยละ", axis=alt.Axis(format=".2f")),
            y=alt.Y(
                "party_display:N",
                sort=party_order,
                title=None,
                axis=alt.Axis(labelLimit=260),
                scale=alt.Scale(
                    paddingInner=y_padding_inner,
                    paddingOuter=y_padding_outer,
                ),
            ),
            yOffset=alt.YOffset("year_label:N", sort=["ครั้งก่อน", "ปีนี้"]),
            color=alt.Color("bar_color:N", scale=None, legend=None),
            opacity=alt.Opacity("year_opacity:Q", scale=None, legend=None),
            tooltip=[
                alt.Tooltip("party_display:N", title="พรรค"),
                alt.Tooltip("year_label:N", title="ช่วงเวลา"),
                alt.Tooltip("votes:Q", title="คะแนน", format=","),
                alt.Tooltip("pct:Q", title="คะแนนร้อยละ", format=".2f"),
            ],
        )
        .properties(
            height=comparison_chart_height(len(party_order), dataset_key=dataset_key),
            background="rgba(0,0,0,0)",
        )
        .configure_view(strokeOpacity=0)
        .configure_axis(
            labelColor="#eef4ff",
            titleColor="#eef4ff",
            domainColor="rgba(159, 194, 255, 0.35)",
            gridColor="rgba(159, 194, 255, 0.14)",
            tickColor="rgba(159, 194, 255, 0.35)",
        )
    )
    st.altair_chart(chart, use_container_width=True)


def render_year_comparison_tab() -> None:
    st.markdown("### เปรียบเทียบครั้งก่อนกับปีนี้")
    st.caption("แสดงเฉพาะพรรคที่มีอยู่ทั้งสองช่วงเวลา และนับ ก้าวไกล เป็นพรรคเดียวกับ ประชาชน")

    for dataset_key in ("constituency", "party_list"):
        comparison_df, comparison_table = build_year_comparison(dataset_key)
        render_year_comparison_chart(
            comparison_df,
            DATASET_CONFIG[dataset_key]["label"],
            dataset_key=dataset_key,
        )
        if comparison_table.empty:
            continue
        st.dataframe(comparison_table, use_container_width=True, hide_index=True)


def build_battleground_ranked(
    dataset_key: str,
    *,
    threshold: float,
    geojson: dict[str, Any],
    limit: int = 10,
) -> pd.DataFrame:
    source_df, vote_cols = load_tambon_dataset(dataset_key)
    tambon_winners, good_matches, _unmatched, _map_geojson, _party_colors = prepare_results(
        source_df, vote_cols, geojson, threshold
    )
    if tambon_winners.empty or good_matches.empty:
        return pd.DataFrame()

    focus_match_mask = good_matches.apply(
        lambda row: is_focus_location(row["official_amphoe"], row["official_tambon"]),
        axis=1,
    )
    focus_matches = good_matches[focus_match_mask].copy()
    focus_tambon_winners = aggregate_matched_rows(focus_matches, vote_cols)
    if focus_tambon_winners.empty:
        return pd.DataFrame()

    ranked = (
        focus_tambon_winners.sort_values(
            ["margin_pct_valid", "margin_votes", "winner_votes"],
            ascending=[True, True, False],
        )
        .head(limit)
        .copy()
    )
    ranked.insert(0, "อันดับ", range(1, len(ranked) + 1))
    ranked["tambon_label"] = ranked.apply(
        lambda row: f"{int(row['อันดับ'])}. {row['official_tambon']}",
        axis=1,
    )
    return ranked


def build_battleground_table(
    dataset_key: str,
    *,
    threshold: float,
    geojson: dict[str, Any],
    limit: int = 10,
) -> pd.DataFrame:
    ranked = build_battleground_ranked(
        dataset_key,
        threshold=threshold,
        geojson=geojson,
        limit=limit,
    )
    if ranked.empty:
        return pd.DataFrame()

    return ranked[
        [
            "อันดับ",
            "official_amphoe",
            "official_tambon",
            "winner_party",
            "runner_up_party",
            "margin_votes",
            "margin_pct_valid",
            "winner_votes",
            "runner_up_votes",
            "turnout_pct",
        ]
    ].rename(
        columns={
            "official_amphoe": "อำเภอ",
            "official_tambon": "ตำบล",
            "winner_party": "ผู้ชนะ",
            "runner_up_party": "รองชนะ",
            "margin_votes": "ส่วนต่าง",
            "margin_pct_valid": "ส่วนต่าง (%)",
            "winner_votes": "คะแนนผู้ชนะ",
            "runner_up_votes": "คะแนนรอง",
            "turnout_pct": "ใช้สิทธิ (%)",
        }
    )


def render_battleground_chart(ranked_df: pd.DataFrame, title: str) -> None:
    st.markdown(f"#### {title}")
    if ranked_df.empty:
        st.info("ยังไม่มีข้อมูล battleground สำหรับกราฟนี้")
        return

    chart_rows = []
    for row in ranked_df.to_dict("records"):
        chart_rows.append(
            {
                "tambon_label": row["tambon_label"],
                "อันดับ": row["อันดับ"],
                "อำเภอ": row["official_amphoe"],
                "ตำบล": row["official_tambon"],
                "เส้น": "ผู้ชนะ",
                "พรรค": display_party_name(row["winner_party"]),
                "คะแนน": to_int(row["winner_votes"]),
                "ส่วนต่าง": to_int(row["margin_votes"]),
            }
        )
        chart_rows.append(
            {
                "tambon_label": row["tambon_label"],
                "อันดับ": row["อันดับ"],
                "อำเภอ": row["official_amphoe"],
                "ตำบล": row["official_tambon"],
                "เส้น": "รองชนะ",
                "พรรค": display_party_name(row["runner_up_party"]),
                "คะแนน": to_int(row["runner_up_votes"]),
                "ส่วนต่าง": to_int(row["margin_votes"]),
            }
        )

    chart_df = pd.DataFrame(chart_rows)
    tambon_order = ranked_df["tambon_label"].tolist()
    series_colors = {"ผู้ชนะ": "#f58220", "รองชนะ": "#9fc2ff"}
    chart_df["series_color"] = chart_df["เส้น"].map(series_colors)

    chart = (
        alt.Chart(chart_df)
        .mark_bar(cornerRadiusTopLeft=5, cornerRadiusTopRight=5, size=26)
        .encode(
            x=alt.X(
                "tambon_label:N",
                sort=tambon_order,
                title=None,
                axis=alt.Axis(labelAngle=-18, labelLimit=180),
            ),
            y=alt.Y("คะแนน:Q", title="คะแนน"),
            xOffset=alt.XOffset("เส้น:N", sort=["ผู้ชนะ", "รองชนะ"]),
            color=alt.Color("series_color:N", scale=None, legend=None),
            tooltip=[
                alt.Tooltip("อันดับ:Q", title="อันดับ"),
                alt.Tooltip("อำเภอ:N", title="อำเภอ"),
                alt.Tooltip("ตำบล:N", title="ตำบล"),
                alt.Tooltip("เส้น:N", title="ชุดข้อมูล"),
                alt.Tooltip("พรรค:N", title="พรรค"),
                alt.Tooltip("คะแนน:Q", title="คะแนน", format=","),
                alt.Tooltip("ส่วนต่าง:Q", title="ส่วนต่าง", format=","),
            ],
        )
        .properties(height=340, background="rgba(0,0,0,0)")
        .configure_view(strokeOpacity=0)
        .configure_axis(
            labelColor="#eef4ff",
            titleColor="#eef4ff",
            domainColor="rgba(159, 194, 255, 0.35)",
            gridColor="rgba(159, 194, 255, 0.14)",
            tickColor="rgba(159, 194, 255, 0.35)",
        )
    )
    st.altair_chart(chart, use_container_width=True)


def render_battleground_tab(*, threshold: float, geojson: dict[str, Any]) -> None:
    st.markdown("### Top 10 Battleground Tambons")
    st.caption("เรียงจากตำบลที่มีส่วนต่างคะแนนน้อยที่สุดในพื้นที่ที่แสดงบนแผนที่")

    constituency_ranked = build_battleground_ranked(
        "constituency",
        threshold=threshold,
        geojson=geojson,
        limit=10,
    )
    party_list_ranked = build_battleground_ranked(
        "party_list",
        threshold=threshold,
        geojson=geojson,
        limit=10,
    )
    constituency_table = build_battleground_table(
        "constituency",
        threshold=threshold,
        geojson=geojson,
        limit=10,
    )
    party_list_table = build_battleground_table(
        "party_list",
        threshold=threshold,
        geojson=geojson,
        limit=10,
    )

    chart_left_col, chart_right_col = st.columns(2, gap="large")
    with chart_left_col:
        render_battleground_chart(constituency_ranked, "สส.เขต")
    with chart_right_col:
        render_battleground_chart(party_list_ranked, "บัญชีรายชื่อ")

    left_col, right_col = st.columns(2, gap="large")
    with left_col:
        st.markdown("#### สส.เขต")
        if constituency_table.empty:
            st.info("ยังไม่มีข้อมูล battleground สำหรับ สส.เขต")
        else:
            st.dataframe(constituency_table, use_container_width=True, hide_index=True)

    with right_col:
        st.markdown("#### บัญชีรายชื่อ")
        if party_list_table.empty:
            st.info("ยังไม่มีข้อมูล battleground สำหรับ บัญชีรายชื่อ")
        else:
            st.dataframe(party_list_table, use_container_width=True, hide_index=True)


def render_dataset_view(
    dataset_key: str,
    *,
    threshold: float,
    show_only_results: bool,
    geojson: dict[str, Any],
) -> None:
    config = DATASET_CONFIG[dataset_key]
    source_df, vote_cols = load_tambon_dataset(dataset_key)

    tambon_winners, good_matches, unmatched, _map_geojson, _party_colors = prepare_results(
        source_df, vote_cols, geojson, threshold
    )
    if tambon_winners.empty:
        st.warning("ยังไม่มีตำบลที่แมตช์ได้ในชุดข้อมูลนี้")
        return

    focus_match_mask = (
        good_matches.apply(lambda row: is_focus_location(row["official_amphoe"], row["official_tambon"]), axis=1)
        if not good_matches.empty
        else pd.Series(dtype=bool)
    )
    focus_matches = good_matches[focus_match_mask].copy()
    out_of_focus_matches = good_matches[~focus_match_mask].copy()
    focus_tambon_winners = aggregate_matched_rows(focus_matches, vote_cols)
    all_parties = [party_parts(label)[1] for label in vote_cols]
    focus_party_colors = build_party_colors(all_parties)
    focus_map_geojson = attach_results_to_geojson(
        filter_focus_geojson(geojson),
        focus_tambon_winners,
        focus_party_colors,
    )
    all_vote_breakdown = build_vote_breakdown(source_df, vote_cols)

    if focus_tambon_winners.empty:
        st.warning("ยังไม่มีตำบลในพื้นที่ที่สนใจที่แมตช์ได้")
        return

    display_geojson = focus_map_geojson
    if show_only_results:
        display_geojson = {
            "type": "FeatureCollection",
            "features": [
                feature for feature in focus_map_geojson["features"]
                if feature.get("properties", {}).get("has_result")
            ],
        }

    min_x, min_y, max_x, max_y = geojson_bounds(display_geojson)
    center_lon = (min_x + max_x) / 2
    center_lat = (min_y + max_y) / 2
    chart_height = chart_height_for_rows(len(all_vote_breakdown))

    map_col, chart_col = st.columns([1.0, 1.0], gap="large")
    with map_col:
        st.markdown("### แผนที่")
        deck = build_map(display_geojson, center_lon=center_lon, center_lat=center_lat, zoom=INITIAL_MAP_ZOOM)
        st.pydeck_chart(deck, height=MAP_PANEL_SIZE, key=f"tambon-winner-map-{dataset_key}")
    with chart_col:
        render_vote_histogram(
            all_vote_breakdown,
            title=f"Histogram: {config['label']}",
            party_colors=focus_party_colors,
            fallback_color="#4a72cc",
            height=chart_height,
        )

    csv_export = focus_tambon_winners[
        [
            "official_amphoe",
            "official_tambon",
            "winner_party",
            "winner_votes",
            "runner_up_party",
            "runner_up_votes",
            "margin_votes",
            "turnout_pct",
            "source_rows",
            "avg_match_score",
        ]
    ].rename(
        columns={
            "official_amphoe": "อำเภอ",
            "official_tambon": "ตำบล",
            "winner_party": "ผู้ชนะ",
            "winner_votes": "คะแนนผู้ชนะ",
            "runner_up_party": "รองชนะเลิศ",
            "runner_up_votes": "คะแนนรอง",
            "margin_votes": "ส่วนต่าง",
            "turnout_pct": "ใช้สิทธิ (%)",
            "source_rows": "แถวที่แมตช์",
            "avg_match_score": "คะแนนแมตช์เฉลี่ย",
        }
    )
    with st.expander(f"ดาวน์โหลดผลผู้ชนะรายตำบล: {config['label']}", expanded=False):
        st.download_button(
            "ดาวน์โหลด CSV",
            data=csv_export.to_csv(index=False).encode("utf-8-sig"),
            file_name=config["download_name"],
            mime="text/csv",
            key=f"download-{dataset_key}",
        )

    with st.expander(f"แถวที่อยู่นอกพื้นที่แผนที่หรือแมตช์ไม่สำเร็จ: {config['label']}", expanded=False):
        review = pd.concat([out_of_focus_matches, unmatched], ignore_index=True)
        if review.empty:
            st.info("ไม่มีแถวที่อยู่นอกพื้นที่หรือแมตช์ไม่สำเร็จ")
        else:
            review["bucket"] = review["official_key"].notna().map(
                {True: "อยู่นอกพื้นที่แผนที่", False: "แมตช์ชื่อไม่สำเร็จ"}
            )
            review = review[
                [
                    "bucket",
                    "raw_amphoe",
                    "raw_tambon",
                    "official_amphoe",
                    "official_tambon",
                    "raw_valid_votes",
                    "match_method",
                    "match_score",
                    "best_guess_amphoe",
                    "best_guess_tambon",
                ]
            ].rename(
                columns={
                    "bucket": "สถานะ",
                    "raw_amphoe": "อำเภอจาก OCR",
                    "raw_tambon": "ตำบลจาก OCR",
                    "official_amphoe": "อำเภอที่แมตช์",
                    "official_tambon": "ตำบลที่แมตช์",
                    "raw_valid_votes": "คะแนนรวม",
                    "match_method": "วิธีแมตช์",
                    "match_score": "คะแนนใกล้เคียง",
                    "best_guess_amphoe": "เดาที่ใกล้สุด อำเภอ",
                    "best_guess_tambon": "เดาที่ใกล้สุด ตำบล",
                }
            ).sort_values(["คะแนนใกล้เคียง", "คะแนนรวม"], ascending=[False, False])
            st.dataframe(review, use_container_width=True, hide_index=True)


def prepare_results(
    source_df: pd.DataFrame,
    vote_cols: list[str],
    geojson: dict[str, Any],
    threshold: float,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any], dict[str, list[int]]]:
    officials = official_records_from_geojson(geojson)
    official_lookup = {item.key: item for item in officials}
    amphoe_groups: dict[str, list[OfficialTambon]] = defaultdict(list)
    for item in officials:
        amphoe_groups[item.amphoe_norm].append(item)

    override_map = load_override_map()

    match_rows: list[dict[str, Any]] = []
    for row in source_df.to_dict("records"):
        result = match_raw_row(
            raw_amphoe=row["raw_amphoe"],
            raw_tambon=row["raw_tambon"],
            officials=officials,
            official_lookup=official_lookup,
            amphoe_groups=amphoe_groups,
            override_map=override_map,
            threshold=threshold,
        )
        record = {
            "raw_amphoe": row["raw_amphoe"],
            "raw_tambon": row["raw_tambon"],
            "raw_valid_votes": to_int(row["raw_valid_votes"]),
            "turnout_pct": to_float(row["turnout_pct"]),
            "จำนวนหน่วย": to_int(row["จำนวนหน่วย"]),
            "ผู้มีสิทธิ": to_int(row["ผู้มีสิทธิ"]),
            "มาใช้สิทธิ": to_int(row["มาใช้สิทธิ"]),
            "บัตรใช้ไป": to_int(row["บัตรใช้ไป"]),
            "บัตรดี": to_int(row["บัตรดี"]),
            "บัตรเสีย": to_int(row["บัตรเสีย"]),
            "บัตรไม่เลือก": to_int(row["บัตรไม่เลือก"]),
            "official_key": result.official_key,
            "official_amphoe": result.official_amphoe,
            "official_tambon": result.official_tambon,
            "best_guess_amphoe": result.best_guess_amphoe,
            "best_guess_tambon": result.best_guess_tambon,
            "match_score": result.score,
            "match_method": result.method,
        }
        for col in vote_cols:
            record[col] = to_int(row[col])
        match_rows.append(record)

    matched_df = pd.DataFrame(match_rows)
    good_matches = matched_df[matched_df["official_key"].notna()].copy()
    unmatched = matched_df[matched_df["official_key"].isna()].copy()
    agg = aggregate_matched_rows(good_matches, vote_cols)
    party_colors = build_party_colors(agg["winner_party"].tolist())
    merged_geojson = attach_results_to_geojson(geojson, agg, party_colors)
    return agg, good_matches, unmatched, merged_geojson, party_colors


def main() -> None:
    st.set_page_config(
        page_title="Lampang Election Dashboard",
        page_icon="🗺️",
        layout="wide",
        initial_sidebar_state="collapsed",
    )
    inject_styles()

    threshold = 0.55
    show_only_results = True
    force_refresh = False

    try:
        geojson = fetch_lampang_boundaries(force_refresh=force_refresh)
    except Exception as exc:
        st.error(
            "โหลดขอบเขตตำบลไม่สำเร็จ ขณะนี้แอปต้องใช้การเชื่อมต่ออินเทอร์เน็ตครั้งแรกเพื่อดึงขอบเขตจาก ArcGIS."
        )
        st.exception(exc)
        return

    constituency_tab, party_list_tab, battleground_tab, comparison_tab = st.tabs(
        [
            DATASET_CONFIG["constituency"]["label_long"],
            DATASET_CONFIG["party_list"]["label_long"],
            "Battleground",
            "Compare",
        ]
    )
    with constituency_tab:
        render_dataset_view(
            "constituency",
            threshold=threshold,
            show_only_results=show_only_results,
            geojson=geojson,
        )
    with battleground_tab:
        render_battleground_tab(
            threshold=threshold,
            geojson=geojson,
        )
    with comparison_tab:
        render_year_comparison_tab()
    with party_list_tab:
        render_dataset_view(
            "party_list",
            threshold=threshold,
            show_only_results=show_only_results,
            geojson=geojson,
        )


if __name__ == "__main__":
    main()
