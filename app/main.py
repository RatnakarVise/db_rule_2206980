from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional, Tuple, Dict, Any
import re
import json

app = FastAPI(
    title="MM-IM Remediator (S4HANA Material Document & Stock Tables)"
)

# -----------------------------
# Reference mappings
# -----------------------------

# Core Document Tables
CORE_DOC_MAP: Dict[str, Dict[str, Any]] = {
    "MKPF": {
        "new": "MATDOC",
        "note": "Header data no longer stored separately. Still exists as DDIC object, but only read via CDS view NSDM_DDL_MKPF."
    },
    "MSEG": {
        "new": "MATDOC",
        "note": "Item + header + attributes merged. Proxy CDS: NSDM_DDL_MSEG."
    },
}

# Hybrid Tables (Master Data + Quantities)
HYBRID_MAP: Dict[str, Dict[str, Any]] = {
    "MARC": {"new": "NSDM_V_MARC / NSDM_MIG_MARC / V_MARC_MD", "note": "Plant Data for Material now redirected to CDS views."},
    "MARD": {"new": "NSDM_V_MARD / NSDM_MIG_MARD / V_MARD_MD", "note": "Storage location data no longer persisted."},
    "MCHB": {"new": "NSDM_V_MCHB / NSDM_MIG_MCHB / V_MCHB_MD", "note": "Batch stock quantities derived from MATDOC."},
    "MKOL": {"new": "NSDM_V_MKOL / NSDM_MIG_MKOL / V_MKOL_MD", "note": "Special stocks from vendor redirected."},
    "MSLB": {"new": "NSDM_V_MSLB / NSDM_MIG_MSLB / V_MSLB_MD", "note": "Special stocks with vendor derived from MATDOC."},
    "MSKA": {"new": "NSDM_V_MSKA / NSDM_MIG_MSKA / V_MSKA_MD", "note": "Sales order stock redirected."},
    "MSPR": {"new": "NSDM_V_MSPR / NSDM_MIG_MSPR / V_MSPR_MD", "note": "Project stock aggregated on the fly."},
    "MSKU": {"new": "NSDM_V_MSKU / NSDM_MIG_MSKU / V_MSKU_MD", "note": "Special stocks with customer from MATDOC."},
}

# Replaced Aggregation Tables
AGGR_MAP: Dict[str, Dict[str, Any]] = {
    "MSSA": {"new": "NSDM_V_MSSA", "note": "Customer order totals replaced by CDS view."},
    "MSSL": {"new": "NSDM_V_MSSL", "note": "Special stocks with vendor totals replaced by CDS view."},
    "MSSQ": {"new": "NSDM_V_MSSQ", "note": "Project stock totals replaced by CDS view."},
    "MSTB": {"new": "NSDM_V_MSTB", "note": "Stock in transit replaced by CDS view."},
    "MSTE": {"new": "NSDM_V_MSTE", "note": "Stock in transit (SD Doc) replaced by CDS view."},
    "MSTQ": {"new": "NSDM_V_MSTQ", "note": "Stock in transit for project replaced by CDS view."},
}

# DIMP Split Hybrid Tables
DIMP_MAP: Dict[str, Dict[str, Any]] = {
    "MCSD": {"new": "NSDM_V_MCSD / MCSD_MD", "note": "Customer Stock split: stock → MATDOC, master → MCSD_MD."},
    "MCSS": {"new": "NSDM_V_MCSS / MCSS_MD", "note": "Customer Stock Total split: stock → MATDOC, master → MCSS_MD."},
    "MSCD": {"new": "NSDM_V_MSCD / MSCD_MD", "note": "Customer Stock with Vendor split into MATDOC + MSCD_MD."},
    "MSCS": {"new": "NSDM_V_MSCS / MSCS_MD", "note": "Cust. Stock with Vendor Total split into MATDOC + MSCS_MD."},
    "MSFD": {"new": "NSDM_V_MSFD / MSFD_MD", "note": "Sales Order Stock with Vendor split into MATDOC + MSFD_MD."},
    "MSFS": {"new": "NSDM_V_MSFS / MSFS_MD", "note": "Sales Order Stock with Vendor Total split into MATDOC + MSFS_MD."},
    "MSID": {"new": "NSDM_V_MSID / MSID_MD", "note": "Vendor Stock split into MATDOC + MSID_MD."},
    "MSIS": {"new": "NSDM_V_MSIS / MSIS_MD", "note": "Vendor Stock Total split into MATDOC + MSIS_MD."},
    "MSRD": {"new": "NSDM_V_MSRD / MSRD_MD", "note": "Project Stock with Vendor split into MATDOC + MSRD_MD."},
    "MSRS": {"new": "NSDM_V_MSRS / MSRS_MD", "note": "Project Stock with Vendor Total split into MATDOC + MSRS_MD."},
}

# History Tables
HISTORY_MAP: Dict[str, Dict[str, Any]] = {
    "MARCH": {"new": "NSDM_V_MARCH", "note": "MARC History redirected to CDS."},
    "MARDH": {"new": "NSDM_V_MARDH", "note": "MARD History redirected to CDS."},
    "MCHBH": {"new": "NSDM_V_MCHBH", "note": "MCHB History redirected to CDS."},
    "MKOLH": {"new": "NSDM_V_MKOLH", "note": "MKOL History redirected to CDS."},
    "MSLBH": {"new": "NSDM_V_MSLBH", "note": "MSLB History redirected to CDS."},
    "MSKAH": {"new": "NSDM_V_MSKAH", "note": "MSKA History redirected to CDS."},
    "MSSAH": {"new": "NSDM_V_MSSAH", "note": "MSSA History redirected to CDS."},
    "MSPRH": {"new": "NSDM_V_MSPRH", "note": "MSPR History redirected to CDS."},
    "MSSQH": {"new": "NSDM_V_MSSQH", "note": "MSSQ History redirected to CDS."},
    "MSKUH": {"new": "NSDM_V_MSKUH", "note": "MSKU History redirected to CDS."},
    "MSTBH": {"new": "NSDM_V_MSTBH", "note": "MSTB History redirected to CDS."},
    "MSTEH": {"new": "NSDM_V_MSTEH", "note": "MSTE History redirected to CDS."},
    "MSTQH": {"new": "NSDM_V_MSTQH", "note": "MSTQ History redirected to CDS."},
    "MCSDH": {"new": "NSDM_V_MCSDH", "note": "MCSD History redirected to CDS."},
    "MCSSH": {"new": "NSDM_V_MCSSH", "note": "MCSS History redirected to CDS."},
    "MSCDH": {"new": "NSDM_V_MSCDH", "note": "MSCD History redirected to CDS."},
    "MSFDH": {"new": "NSDM_V_MSFDH", "note": "MSFD History redirected to CDS."},
    "MSIDH": {"new": "NSDM_V_MSIDH", "note": "MSID History redirected to CDS."},
    "MSRDH": {"new": "NSDM_V_MSRDH", "note": "MSRD History redirected to CDS."},
}

# Merge all tables into one map for detection
TABLE_MAP = {**CORE_DOC_MAP, **HYBRID_MAP, **AGGR_MAP, **DIMP_MAP, **HISTORY_MAP}

# -----------------------------
# Regex
# -----------------------------

TABLE_NAMES = sorted(TABLE_MAP.keys(), key=len, reverse=True)
TABLE_RE = re.compile(
    rf"""
    \b(?P<name>{'|'.join(map(re.escape, TABLE_NAMES))})\b
    """,
    re.IGNORECASE | re.VERBOSE
)

# -----------------------------
# Models
# -----------------------------

class Unit(BaseModel):
    pgm_name: str
    inc_name: str
    type: str
    name: Optional[str] = None
    class_implementation: Optional[str] = None
    start_line: Optional[int] = None
    end_line: Optional[int] = None
    code: Optional[str] = ""

# -----------------------------
# Helpers
# -----------------------------

def _add_hit(
    hits: List[dict],
    span: Tuple[int, int],
    target_name: str,
    suggested_statement: str,
    note: Optional[str] = None
):
    meta = {
        "table": target_name,
        "target_type": "Table",
        "target_name": target_name,
        "start_char_in_unit": span[0] if span else None,
        "end_char_in_unit": span[1] if span else None,
        "used_fields": [],
        "ambiguous": False,
        "suggested_statement": suggested_statement,
        "suggested_fields": None
    }
    if note:
        meta["note"] = note
    hits.append(meta)

def find_mm_im_issues(txt: str) -> List[dict]:
    if not txt:
        return []

    issues: List[dict] = []

    for m in TABLE_RE.finditer(txt):
        name = m.group("name").upper()
        info = TABLE_MAP.get(name)
        if info:
            suggested = f"Use {info['new']} instead of {name}."
            _add_hit(issues, m.span(), name, suggested, note=info.get("note"))

    return issues

# -----------------------------
# API
# -----------------------------

@app.post("/remediate-mm-im")
async def remediate_mm_im(units: List[Unit]):
    """
    Input: list of ABAP 'units' with code.
    Output: same structure with appended 'mb_txn_usage' list of remediation suggestions.
    """
    results = []
    for u in units:
        src = u.code or ""
        issues = find_mm_im_issues(src)

        obj = json.loads(u.model_dump_json())
        obj["mb_txn_usage"] = issues
        results.append(obj)
    return results
