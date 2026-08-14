"""
Generate TABMIS budget-execution workbooks and drop them in the intake bucket.

This stands in for a person exporting from TABMIS and uploading the file. It is
NOT part of the pipeline — it only produces the input the pipeline has to cope
with, so every awkward thing here is deliberate.

What makes this source hard has nothing to do with volume and everything to do
with Excel being a document format that happens to hold data:

  a merged title block above the header row, so the header is not row 1
  amounts stored as text with thousand separators, so they look numeric
  dates stored as serial numbers, so 46023 is a date and not an amount
  subtotal rows interleaved with detail, so a naive SUM double-counts
  blank spacer rows between groups
  unit codes that no longer exist in the reference data
  a cumulative column that occasionally goes backwards

Run: python gen_tabmis.py            writes every period to the bucket
     python gen_tabmis.py 2026-03    writes one period
"""

import hashlib
import io
import os
import random
import sys
from datetime import date, datetime

import boto3
import psycopg2
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://seaweedfs-s3:8333")
INTAKE_BUCKET = os.environ.get("INTAKE_BUCKET", "intake")
FISCAL_YEAR = 2026
PERIODS = [f"{FISCAL_YEAR}-{m:02d}" for m in range(1, 9)]  # Jan..Aug

# Excel's day zero. 1899-12-30 rather than 12-31 because Excel keeps Lotus
# 1-2-3's bug of treating 1900 as a leap year.
EXCEL_EPOCH = date(1899, 12, 30)

# Two workbook layouts from one system. They share the identifying columns and
# the budget classification, then diverge: expenditure is broken down by
# funding source and spending sector, revenue by tax type and revenue source.
# There is no "tam ung" on the revenue side — nobody advances a tax receipt.
EXPENSE_HEADERS = [
    "Ky", "Ma DVQHNS", "Ten don vi", "Ma dia ban",
    "Chuong", "Loai", "Khoan", "Muc", "Tieu muc",
    "Ma nguon KP", "Ma linh vuc chi",
    "Du toan giao", "Dieu chinh", "Thuc chi", "Tam ung", "Luy ke",
    "Ngay cap nhat",
]

REVENUE_HEADERS = [
    "Ky", "Ma DVQHNS", "Ten don vi", "Ma dia ban",
    "Chuong", "Loai", "Khoan", "Muc", "Tieu muc",
    "Ma sac thue", "Ma nguon thu",
    "Du toan giao", "Dieu chinh", "Thuc thu", "Luy ke",
    "Ngay cap nhat",
]

# Tax type follows from the revenue line: the chart of accounts already encodes
# which tax a receipt belongs to, so the submitter is not asked to restate it.
TAX_BY_LINE = {
    "1001": "TX01", "1002": "TX01", "1004": "TX01",
    "1052": "TX02", "1053": "TX02",
    "1151": "TX03", "1154": "TX03", "1156": "TX03",
    "1301": "TX04", "1401": "TX05", "1551": "TX06",
    "1601": "TX07", "1602": "TX07", "1701": "TX08",
    "2801": "TX09", "2803": "TX09",
    "2001": "TX10", "2011": "TX10",
    "4902": "TX11", "4949": "TX11",
}

# Unit codes that were never in the reference data — a department that was
# dissolved, and a typo that survived into the export.
GHOST_UNITS = ["1054099", "1054777"]


def excel_serial(d):
    return (d - EXCEL_EPOCH).days


def load_reference():
    conn = psycopg2.connect(
        host=os.environ["DWH_HOST"], dbname=os.environ["DWH_DBNAME"],
        user=os.environ["DWH_USER"], password=os.environ["DWH_PASSWORD"],
        connect_timeout=10,
    )
    try:
        cur = conn.cursor()
        # Collecting agencies (10543xx) appear on revenue rows only; spending
        # units on expenditure rows only. Same table, two populations — a tax
        # office does not have a spending budget in this export, and a school
        # does not collect tax.
        # budget_unit is versioned now, so take the newest version of each unit.
        # The export therefore carries today's names even for old months —
        # which is what a real export generated today would do, and exactly why
        # the warehouse resolves units by CODE and never trusts the name in the
        # file.
        cur.execute("""
            SELECT DISTINCT ON (u.unit_code)
                   u.unit_code, u.unit_name, u.locality_code, u.unit_level, u.parent_code
            FROM refdata.budget_unit u
            WHERE u.unit_code NOT LIKE '10543%%'
            ORDER BY u.unit_code, u.valid_from DESC
        """)
        units = cur.fetchall()

        cur.execute("""
            SELECT DISTINCT ON (u.unit_code)
                   u.unit_code, u.unit_name, u.locality_code, u.unit_level, u.parent_code
            FROM refdata.budget_unit u
            WHERE u.unit_code LIKE '10543%%'
            ORDER BY u.unit_code, u.valid_from DESC
        """)
        agencies = cur.fetchall()

        cur.execute("""
            SELECT line_code, category_code, subcategory_code, item_code
            FROM refdata.budget_line
            WHERE fiscal_year = %s AND flow_type = 'expense'
            ORDER BY line_code
        """, (FISCAL_YEAR,))
        lines = cur.fetchall()

        cur.execute("""
            SELECT line_code, category_code, subcategory_code, item_code
            FROM refdata.budget_line
            WHERE fiscal_year = %s AND flow_type = 'revenue'
            ORDER BY line_code
        """, (FISCAL_YEAR,))
        rev_lines = cur.fetchall()

        # The "not applicable" members exist so the warehouse can key a
        # revenue row against a dimension it does not use. A real expenditure
        # row must never be assigned one, so they are kept out of the pool the
        # generator draws from.
        cur.execute("""SELECT funding_code FROM refdata.funding_source
                       WHERE funding_group <> 'n/a' ORDER BY funding_code""")
        fundings = [r[0] for r in cur.fetchall()]

        cur.execute("""SELECT sector_code FROM refdata.expense_sector
                       WHERE sector_code <> 'LV00' ORDER BY sector_code""")
        sectors = [r[0] for r in cur.fetchall()]
        return units, agencies, lines, rev_lines, fundings, sectors
    finally:
        conn.close()


def chapter_for(unit_level, parent_code):
    """Managing body follows the parent department, not the spending nature."""
    mapping = {
        "1054010": "422", "1054011": "423", "1054012": "424", "1054013": "426",
        "1054014": "428", "1054015": "430", "1054016": "432", "1054017": "434",
        "1054018": "436", "1054019": "438", "1054020": "440", "1054021": "442",
        "1054022": "444", "1054023": "446", "1054024": "448",
        "1054026": "460", "1054027": "462",
    }
    if unit_level == "district":
        return "470"
    return mapping.get(parent_code, "400")


def sector_for(unit_name):
    if "THPT" in unit_name or "THCS" in unit_name or "Giao duc" in unit_name:
        return "LV03"
    if "Y te" in unit_name:
        return "LV05"
    if "Van hoa" in unit_name:
        return "LV06"
    if "Cong an" in unit_name:
        return "LV02"
    if "Quan su" in unit_name:
        return "LV01"
    if "du an" in unit_name or "Nong nghiep" in unit_name or "Giao thong" in unit_name:
        return "LV10"
    return "LV11"


BASE_ALLOCATION = {
    "6001": 900_000_000, "6003": 180_000_000, "6051": 120_000_000,
    "6101": 90_000_000,  "6103": 40_000_000,  "6112": 210_000_000,
    "6301": 190_000_000, "6302": 34_000_000,  "6303": 18_000_000,
    "9051": 450_000_000, "9062": 800_000_000, "7001": 260_000_000,
}

_PLAN_CACHE = {}


def unit_plan(unit_code, unit_level, lines, fundings):
    """
    A unit's spending plan for the whole year, stable across every period.

    Seeded from the unit code rather than the period, for two reasons. A body
    does not spend on a different random set of budget lines each month — it
    has a plan. And the annual allocation cannot be re-rolled monthly, or the
    same line shows a different budget in January and February.

    Returning the twelve monthly execution factors up front is what lets the
    cumulative column be a real cumulative sum instead of a fresh guess.
    """
    if unit_code in _PLAN_CACHE:
        return _PLAN_CACHE[unit_code]

    r = random.Random("plan|" + unit_code)
    n_lines = {"province": 34, "department": 26, "district": 18}.get(unit_level, 12)
    scale = {"province": 40, "department": 12, "district": 6}.get(unit_level, 1.6)

    plan = []
    for line_code, cat, subcat, item in r.sample(lines, min(n_lines, len(lines))):
        # Weight the common sources heavily, then flat for whatever else the
        # reference data carries — so adding a funding source never breaks this.
        weights = ([70, 14, 4, 4, 6, 2] + [1] * len(fundings))[:len(fundings)]
        funding = r.choices(fundings, weights=weights, k=1)[0]
        base = BASE_ALLOCATION.get(line_code, 60_000_000)
        allocated = int(base * scale * r.uniform(0.85, 1.15) / 1000) * 1000
        adjusted = 0
        if r.random() < 0.12:
            adjusted = int(allocated * r.uniform(-0.15, 0.25) / 1000) * 1000
        factors = [r.uniform(0.55, 1.35) for _ in range(12)]
        plan.append((line_code, cat, subcat, item, funding,
                     allocated, adjusted, factors))

    _PLAN_CACHE[unit_code] = plan
    return plan


def build_rows(period, units, lines, fundings, sectors, rnd, restated=False):
    """
    One period's worth of detail rows, plus the subtotal rows a real export
    carries. Returns a list of dicts; the writer decides how they land in cells.
    """
    month = int(period.split("-")[1])
    rows = []

    for unit_code, unit_name, locality_code, unit_level, parent_code in units:
        # The merged school stops reporting from July.
        if unit_code == "1054231" and month >= 7:
            continue

        chapter = chapter_for(unit_level, parent_code)
        sector = sector_for(unit_name)
        plan = unit_plan(unit_code, unit_level, lines, fundings)

        unit_rows = []
        for line_code, cat, subcat, item, funding, allocated, adjusted, factors in plan:
            monthly_target = (allocated + adjusted) / 12

            def spend(m):
                value = int(monthly_target * factors[m - 1] / 1000) * 1000
                if restated:
                    # A restatement adjusts figures; it does not reshuffle them.
                    value = int(value * 1.03 / 1000) * 1000
                return value

            executed = spend(month)

            # Cumulative means cumulative: the sum of what came before, not an
            # independent guess. Deriving it any other way makes the column
            # wander backwards and trips a consistency rule that is doing its
            # job — the data would be wrong, not the rule.
            ytd = sum(spend(m) for m in range(1, month + 1))

            advance = 0
            if factors[month - 1] > 1.28:
                advance = int(executed * 0.18 / 1000) * 1000

            unit_rows.append({
                "kind": "detail",
                "period": period,
                "unit_code": unit_code,
                "unit_name": unit_name,
                "locality_code": locality_code,
                "chapter": chapter, "cat": cat, "subcat": subcat, "item": item,
                "line_code": line_code,
                "funding": funding,
                "sector": sector,
                "allocated": allocated, "adjusted": adjusted,
                "executed": executed, "advance": advance, "ytd": ytd,
            })

        rows.extend(unit_rows)

        # Per-unit subtotal, exactly as the export produces it. Nothing marks it
        # as a subtotal except the words in the unit-name column.
        detail = unit_rows
        if detail:
            rows.append({
                "kind": "subtotal",
                "period": period,
                "unit_code": unit_code,
                "unit_name": f"Cong: {unit_name}",
                "locality_code": locality_code,
                "chapter": chapter, "cat": "", "subcat": "", "item": "",
                "line_code": "", "funding": "", "sector": sector,
                "allocated": sum(r["allocated"] for r in detail),
                "adjusted": sum(r["adjusted"] for r in detail),
                "executed": sum(r["executed"] for r in detail),
                "advance": sum(r["advance"] for r in detail),
                "ytd": sum(r["ytd"] for r in detail),
            })
            rows.append({"kind": "blank"})

    # Ghost units: codes the reference data has never heard of.
    for ghost in GHOST_UNITS:
        for line_code, cat, subcat, item in rnd.sample(lines, 4):
            rows.append({
                "kind": "detail",
                "period": period,
                "unit_code": ghost,
                "unit_name": "Don vi chua co trong danh muc",
                "locality_code": "LOC01",
                "chapter": "400", "cat": cat, "subcat": subcat, "item": item,
                "line_code": line_code, "funding": "NKP01", "sector": "LV11",
                "allocated": 50_000_000, "adjusted": 0,
                "executed": 4_000_000, "advance": 0, "ytd": 4_000_000 * month,
            })

    # Grand total for the whole province — the row that doubles every SUM.
    detail_all = [r for r in rows if r["kind"] == "detail"]
    rows.append({
        "kind": "grand_total",
        "period": period,
        "unit_code": "",
        "unit_name": "TONG CONG TOAN TINH",
        "locality_code": "", "chapter": "", "cat": "", "subcat": "", "item": "",
        "line_code": "", "funding": "", "sector": "",
        "allocated": sum(r["allocated"] for r in detail_all),
        "adjusted": sum(r["adjusted"] for r in detail_all),
        "executed": sum(r["executed"] for r in detail_all),
        "advance": sum(r["advance"] for r in detail_all),
        "ytd": sum(r["ytd"] for r in detail_all),
    })
    return rows


def build_revenue_rows(period, agencies, rev_lines, rnd):
    """
    A month of revenue collection.

    The grain is wider than expenditure: a district tax office collects several
    taxes, and the locality varies per row rather than being a property of the
    agency. That is why locality had to join the fact's key.

    Customs and the treasury collect a narrow, different set — modelled because
    a revenue report broken down by agency that shows every agency collecting
    everything is obviously synthetic at a glance.
    """
    month = int(period.split("-")[1])
    rows = []

    for unit_code, unit_name, locality_code, unit_level, parent_code in agencies:
        if "Hai quan" in unit_name:
            allowed = [l for l in rev_lines if l[0] in ("1002", "1301")]
            source = "NT02"
        elif "Kho bac" in unit_name:
            allowed = [l for l in rev_lines if l[0] in ("2801", "2803", "4902")]
            source = "NT01"
        else:
            allowed = [l for l in rev_lines if l[0] not in ("1002",)]
            source = "NT01"
        if not allowed:
            continue

        chapter = "400"
        r = random.Random("rev|" + unit_code)
        n_lines = 16 if unit_level == "department" else 11
        chosen = r.sample(allowed, min(n_lines, len(allowed)))
        scale = 26.0 if unit_level == "department" else 4.0

        unit_rows = []
        for line_code, cat, subcat, item in chosen:
            base = {
                "1001": 3_200_000_000, "1052": 2_400_000_000,
                "1151": 1_600_000_000, "2001": 5_800_000_000,
                "1601": 700_000_000,   "1602": 520_000_000,
            }.get(line_code, 400_000_000)

            allocated = int(base * scale * r.uniform(0.85, 1.15) / 1000) * 1000
            adjusted = 0
            if r.random() < 0.10:
                adjusted = int(allocated * r.uniform(-0.10, 0.20) / 1000) * 1000
            factors = [r.uniform(0.6, 1.4) for _ in range(12)]

            def collect(m):
                return int((allocated + adjusted) / 12 * factors[m - 1] / 1000) * 1000

            executed = collect(month)
            ytd = sum(collect(m) for m in range(1, month + 1))

            unit_rows.append({
                "kind": "detail", "period": period,
                "unit_code": unit_code, "unit_name": unit_name,
                "locality_code": locality_code,
                "chapter": chapter, "cat": cat, "subcat": subcat, "item": item,
                "line_code": line_code,
                "tax_type": TAX_BY_LINE.get(line_code, "TX11"),
                "revenue_source": source,
                "allocated": allocated, "adjusted": adjusted,
                "executed": executed, "advance": 0, "ytd": ytd,
            })

        rows.extend(unit_rows)
        rows.append({
            "kind": "subtotal", "period": period,
            "unit_code": unit_code, "unit_name": f"Cong: {unit_name}",
            "locality_code": locality_code,
            "chapter": chapter, "cat": "", "subcat": "", "item": "",
            "line_code": "", "tax_type": "", "revenue_source": "",
            "allocated": sum(x["allocated"] for x in unit_rows),
            "adjusted": sum(x["adjusted"] for x in unit_rows),
            "executed": sum(x["executed"] for x in unit_rows),
            "advance": 0,
            "ytd": sum(x["ytd"] for x in unit_rows),
        })
        rows.append({"kind": "blank"})

    detail_all = [x for x in rows if x["kind"] == "detail"]
    rows.append({
        "kind": "grand_total", "period": period,
        "unit_code": "", "unit_name": "TONG CONG THU NSNN TOAN TINH",
        "locality_code": "", "chapter": "", "cat": "", "subcat": "", "item": "",
        "line_code": "", "tax_type": "", "revenue_source": "",
        "allocated": sum(x["allocated"] for x in detail_all),
        "adjusted": sum(x["adjusted"] for x in detail_all),
        "executed": sum(x["executed"] for x in detail_all),
        "advance": 0,
        "ytd": sum(x["ytd"] for x in detail_all),
    })
    return rows


def write_workbook(period, rows, rnd, restated=False, flow="expense"):
    revenue = flow == "revenue"
    headers = REVENUE_HEADERS if revenue else EXPENSE_HEADERS

    wb = Workbook()
    ws = wb.active
    ws.title = "Thu NSNN" if revenue else "Chi NSNN"

    bold = Font(bold=True)
    centre = Alignment(horizontal="center", vertical="center")
    thin = Side(style="thin", color="BFBFBF")
    box = Border(left=thin, right=thin, top=thin, bottom=thin)
    head_fill = PatternFill("solid", fgColor="DCE6F1")
    last_col = get_column_letter(len(headers))

    # ── title block: merged cells above the header, exactly what makes a
    #    naive "header is row 1" reader produce nonsense ────────────────────
    ws.merge_cells(f"A1:{last_col}1")
    ws["A1"] = "UY BAN NHAN DAN TINH HUNG YEN"
    ws["A1"].font = Font(bold=True, size=12)
    ws["A1"].alignment = centre

    ws.merge_cells(f"A2:{last_col}2")
    ws["A2"] = ("BAO CAO THU NGAN SACH NHA NUOC" if revenue
                else "BAO CAO CHI NGAN SACH NHA NUOC")
    ws["A2"].font = Font(bold=True, size=14)
    ws["A2"].alignment = centre

    ws.merge_cells(f"A3:{last_col}3")
    ws["A3"] = f"Ky bao cao: thang {int(period.split('-')[1])} nam {period.split('-')[0]}"
    if restated:
        ws["A3"] = ws["A3"].value + "  (BAO CAO BO SUNG - THAY THE BAN TRUOC)"
    ws["A3"].alignment = centre

    # Report date written as a raw serial with a date format — the cell LOOKS
    # like a date in Excel and reads as 46000-odd to anything else.
    ws["A4"] = "Ngay lap bieu:"
    ws["B4"] = excel_serial(date(FISCAL_YEAR, int(period.split("-")[1]), 28))
    ws["B4"].number_format = "dd/mm/yyyy"

    ws.append([])  # row 5 spacer

    # ── header row (row 6) ───────────────────────────────────────────────
    ws.append(headers)
    for c in range(1, len(headers) + 1):
        cell = ws.cell(row=6, column=c)
        cell.font = bold
        cell.alignment = centre
        cell.fill = head_fill
        cell.border = box

    # ── data ─────────────────────────────────────────────────────────────
    text_amount_every = 43   # a slice of amounts typed as text, thousand-separated
    n = 0
    for r in rows:
        if r["kind"] == "blank":
            ws.append([])
            continue

        n += 1

        def amount(v):
            # Some cells arrive as text. Indistinguishable to the eye, fatal to
            # a reader that assumes numeric.
            if r["kind"] == "detail" and n % text_amount_every == 0:
                return f"{v:,}".replace(",", ".")
            return v

        ytd = r["ytd"]
        # A cumulative figure that went backwards — the consistency rule in
        # B5.3 exists because this happens in real submissions.
        if r["kind"] == "detail" and n % 271 == 0:
            ytd = int(ytd * 0.6)

        updated = excel_serial(date(FISCAL_YEAR, int(period.split("-")[1]), 28))

        common = [
            r["period"], r["unit_code"], r["unit_name"], r["locality_code"],
            r["chapter"], r["cat"], r["subcat"], r["item"], r["line_code"],
        ]
        if revenue:
            ws.append(common + [
                r["tax_type"], r["revenue_source"],
                amount(r["allocated"]), amount(r["adjusted"]),
                amount(r["executed"]), amount(ytd),
                updated,
            ])
        else:
            ws.append(common + [
                r["funding"], r["sector"],
                amount(r["allocated"]), amount(r["adjusted"]),
                amount(r["executed"]), amount(r["advance"]), amount(ytd),
                updated,
            ])
        ws.cell(row=ws.max_row, column=len(headers)).number_format = "dd/mm/yyyy"

    widths = [9, 11, 34, 10, 8, 7, 8, 7, 9, 12, 13, 16, 14, 16, 14, 16, 13]
    for i, w in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(i)].width = w
    ws.freeze_panes = "A7"

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def main():
    wanted = sys.argv[1:] or PERIODS
    units, agencies, lines, rev_lines, fundings, sectors = load_reference()
    print(f"reference: {len(units)} spending units, {len(agencies)} collecting "
          f"agencies, {len(lines)} expense lines, {len(rev_lines)} revenue lines",
          flush=True)

    s3 = boto3.client(
        "s3", endpoint_url=S3_ENDPOINT,
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY", "warehouse"),
        aws_secret_access_key=os.environ.get("S3_SECRET_KEY", "warehouse"),
        region_name="us-east-1",
    )
    try:
        s3.create_bucket(Bucket=INTAKE_BUCKET)
    except Exception:
        pass

    total_rows = 0
    for period in wanted:
        # Seeded per period so re-running produces byte-identical files.
        rnd = random.Random(int(period.replace("-", "")))
        rows = build_rows(period, units, lines, fundings, sectors, rnd)
        blob = write_workbook(period, rows, rnd)
        detail = sum(1 for r in rows if r["kind"] == "detail")
        total_rows += detail

        key = f"tabmis/{period}/chi-nsnn-{period}.xlsx"
        s3.put_object(Bucket=INTAKE_BUCKET, Key=key, Body=blob)
        print(f"  {key:44} {len(blob)//1024:5d} KB  {detail:6d} dong chi tiet  "
              f"sha256={hashlib.sha256(blob).hexdigest()[:12]}", flush=True)

        # March is resubmitted with corrected figures a month later. This is the
        # documented "nop bo sung sau chot ky" case, and the only way to prove
        # the loader replaces a period instead of adding to it.
        if period == "2026-03":
            rnd2 = random.Random(20260399)
            rows2 = build_rows(period, units, lines, fundings, sectors, rnd2,
                               restated=True)
            blob2 = write_workbook(period, rows2, rnd2, restated=True)
            key2 = f"tabmis/{period}/chi-nsnn-{period}-bosung.xlsx"
            s3.put_object(Bucket=INTAKE_BUCKET, Key=key2, Body=blob2)
            print(f"  {key2:44} {len(blob2)//1024:5d} KB  "
                  f"{sum(1 for r in rows2 if r['kind'] == 'detail'):6d} dong chi tiet  "
                  f"(BAN BO SUNG)", flush=True)

        # The revenue workbook for the same month. Same source, same submitter,
        # same intake folder — a different sheet out of the same system.
        rev_rows = build_revenue_rows(period, agencies, rev_lines, rnd)
        rev_blob = write_workbook(period, rev_rows, rnd, flow="revenue")
        rev_detail = sum(1 for r in rev_rows if r["kind"] == "detail")
        total_rows += rev_detail

        rev_key = f"tabmis/{period}/thu-nsnn-{period}.xlsx"
        s3.put_object(Bucket=INTAKE_BUCKET, Key=rev_key, Body=rev_blob)
        print(f"  {rev_key:44} {len(rev_blob)//1024:5d} KB  {rev_detail:6d} dong thu     "
              f"sha256={hashlib.sha256(rev_blob).hexdigest()[:12]}", flush=True)

    print(f"\ntong cong {total_rows} dong chi tiet qua {len(wanted)} ky", flush=True)


if __name__ == "__main__":
    main()
