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

S3_ENDPOINT = os.environ.get(
    "S3_ENDPOINT", "http://sw-seaweedfs-s3.stc-hy.svc.cluster.local:8333"
)
INTAKE_BUCKET = os.environ.get("INTAKE_BUCKET", "intake")
FISCAL_YEAR = 2026
PERIODS = [f"{FISCAL_YEAR}-{m:02d}" for m in range(1, 9)]  # Jan..Aug

# Excel's day zero. 1899-12-30 rather than 12-31 because Excel keeps Lotus
# 1-2-3's bug of treating 1900 as a leap year.
EXCEL_EPOCH = date(1899, 12, 30)

HEADERS = [
    "Ky", "Ma DVQHNS", "Ten don vi", "Ma dia ban",
    "Chuong", "Loai", "Khoan", "Muc", "Tieu muc",
    "Ma nguon KP", "Ma linh vuc chi",
    "Du toan giao", "Dieu chinh", "Thuc chi", "Tam ung", "Luy ke",
    "Ngay cap nhat",
]

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
        cur.execute("""
            SELECT u.unit_code, u.unit_name, u.locality_code, u.unit_level, u.parent_code
            FROM refdata.budget_unit u ORDER BY u.unit_code
        """)
        units = cur.fetchall()

        cur.execute("""
            SELECT line_code, category_code, subcategory_code, item_code
            FROM refdata.budget_line
            WHERE fiscal_year = %s AND flow_type = 'expense'
            ORDER BY line_code
        """, (FISCAL_YEAR,))
        lines = cur.fetchall()

        cur.execute("SELECT funding_code FROM refdata.funding_source ORDER BY funding_code")
        fundings = [r[0] for r in cur.fetchall()]

        cur.execute("SELECT sector_code FROM refdata.expense_sector ORDER BY sector_code")
        sectors = [r[0] for r in cur.fetchall()]
        return units, lines, fundings, sectors
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

        # Bigger bodies spend on more lines than a commune-level school.
        n_lines = {"province": 34, "department": 26, "district": 18}.get(unit_level, 12)
        chosen = rnd.sample(lines, min(n_lines, len(lines)))

        unit_scale = {"province": 40, "department": 12, "district": 6}.get(unit_level, 1.6)

        unit_rows = []
        for line_code, cat, subcat, item in chosen:
            funding = rnd.choices(fundings, weights=[70, 14, 4, 4, 6, 2], k=1)[0]

            base = {
                "6001": 900_000_000, "6003": 180_000_000, "6051": 120_000_000,
                "6101": 90_000_000,  "6103": 40_000_000,  "6112": 210_000_000,
                "6301": 190_000_000, "6302": 34_000_000,  "6303": 18_000_000,
                "9051": 450_000_000, "9062": 800_000_000, "7001": 260_000_000,
            }.get(line_code, 60_000_000)

            allocated = int(base * unit_scale * rnd.uniform(0.85, 1.15) / 1000) * 1000
            adjusted = 0
            if rnd.random() < 0.12:
                adjusted = int(allocated * rnd.uniform(-0.15, 0.25) / 1000) * 1000

            # Execution ramps through the year and is never perfectly even.
            monthly_target = (allocated + adjusted) / 12
            executed = int(monthly_target * rnd.uniform(0.55, 1.35) / 1000) * 1000
            if restated:
                executed = int(executed * rnd.uniform(0.97, 1.06) / 1000) * 1000

            advance = 0
            if rnd.random() < 0.09:
                advance = int(executed * rnd.uniform(0.05, 0.30) / 1000) * 1000

            ytd = int(executed * month * rnd.uniform(0.92, 1.05) / 1000) * 1000

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


def write_workbook(period, rows, rnd, restated=False):
    wb = Workbook()
    ws = wb.active
    ws.title = "Chi NSNN"

    bold = Font(bold=True)
    centre = Alignment(horizontal="center", vertical="center")
    thin = Side(style="thin", color="BFBFBF")
    box = Border(left=thin, right=thin, top=thin, bottom=thin)
    head_fill = PatternFill("solid", fgColor="DCE6F1")
    last_col = get_column_letter(len(HEADERS))

    # ── title block: merged cells above the header, exactly what makes a
    #    naive "header is row 1" reader produce nonsense ────────────────────
    ws.merge_cells(f"A1:{last_col}1")
    ws["A1"] = "UY BAN NHAN DAN TINH HUNG YEN"
    ws["A1"].font = Font(bold=True, size=12)
    ws["A1"].alignment = centre

    ws.merge_cells(f"A2:{last_col}2")
    ws["A2"] = "BAO CAO CHI NGAN SACH NHA NUOC"
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
    ws.append(HEADERS)
    for c in range(1, len(HEADERS) + 1):
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

        ws.append([
            r["period"], r["unit_code"], r["unit_name"], r["locality_code"],
            r["chapter"], r["cat"], r["subcat"], r["item"], r["line_code"],
            r["funding"], r["sector"],
            amount(r["allocated"]), amount(r["adjusted"]),
            amount(r["executed"]), amount(r["advance"]), amount(ytd),
            updated,
        ])
        ws.cell(row=ws.max_row, column=len(HEADERS)).number_format = "dd/mm/yyyy"

    widths = [9, 11, 34, 10, 8, 7, 8, 7, 9, 12, 13, 16, 14, 16, 14, 16, 13]
    for i, w in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(i)].width = w
    ws.freeze_panes = "A7"

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def main():
    wanted = sys.argv[1:] or PERIODS
    units, lines, fundings, sectors = load_reference()
    print(f"reference: {len(units)} units, {len(lines)} lines, "
          f"{len(fundings)} funding sources", flush=True)

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

    print(f"\ntong cong {total_rows} dong chi tiet qua {len(wanted)} ky", flush=True)


if __name__ == "__main__":
    main()
