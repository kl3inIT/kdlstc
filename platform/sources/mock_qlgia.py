"""
Mock QL Gia — stands in for the provincial Price Management system.

Contract it honours (source profile B4.1):
    transport    REST, page-based
    business key commodity x locality x survey period x outlet
    cursor       lastModified
    cadence      by SURVEY period, which is not the calendar period

IMPORTANT — the source publishes SURVEY POINTS, not summaries.

A surveyor visits several outlets for the same commodity in the same district
and records a price at each one. So one commodity x locality x period is 3 to 5
rows here, while the report needs a single number. Collapsing those rows into
one average is the warehouse's job, and it only shows up in the demo because
the source refuses to do it first.

That difference is also why fact_price carries survey_points: averaging an
average is wrong. A district surveyed at five outlets and one surveyed at a
single outlet cannot be combined by adding two numbers and halving them — any
roll-up has to weight by how many points went in.

Payload keys are camelCase, deliberately different from warehouse column
names, so renaming source vocabulary stays visible work.

Standard library only, so this runs straight on python:3.12-slim with no
image build.

Endpoints
    GET  /health
    GET  /api/periods
    GET  /api/prices?updatedSince=<ISO>&period=<code>&page=<n>&pageSize=<n>
    POST /admin/schema-drift?mode=break|add|reset
"""

import json
import os
import random
import re
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

TZ = timezone(timedelta(hours=7))

# Source-side item codes. These are NOT warehouse codes — they have to go
# through metadata.mapping_rules to become CMD*.
ITEMS = [
    ("QLG-GAO-TE", "Gao te thuong", "kg", 18_000),
    ("QLG-GAO-TAM", "Gao tam thom", "kg", 24_000),
    ("QLG-LON-HOI", "Thit lon hoi", "kg", 62_000),
    ("QLG-BO-THAN", "Thit bo than", "kg", 285_000),
    ("QLG-DUONG", "Duong kinh trang", "kg", 21_000),
    ("QLG-XM-PCB40", "Xi mang PCB40", "tan", 1_450_000),
    ("QLG-THEP-CB300", "Thep xay dung CB300", "tan", 14_800_000),
    ("QLG-CAT-VANG", "Cat vang xay dung", "m3", 420_000),
    ("QLG-URE", "Phan bon Ure", "tan", 10_500_000),
    ("QLG-RON95", "Xang RON95-III", "lit", 21_300),
    # No mapping rule exists for these two -> metadata.mapping_rejections
    ("QLG-DAU-DO", "Dau DO 0,05S", "lit", 19_800),
    ("QLG-GAS", "Gas LPG dan dung", "binh", 425_000),
]

AREAS = [
    "QLG-TPHY", "QLG-VLAM", "QLG-VGIANG", "QLG-YMY", "QLG-MHAO",
    "QLG-ATHI", "QLG-KCHAU", "QLG-KDONG", "QLG-TLU", "QLG-PCU",
]

# Kinds of outlet a surveyor visits. Prices differ systematically between
# them — a supermarket is not a wet market — which is exactly why the average
# needs several points to mean anything.
OUTLET_KINDS = [
    ("CHO", "Cho trung tam", 1.000),
    ("ST",  "Sieu thi",      1.045),
    ("DL",  "Dai ly",        0.975),
    ("CH",  "Cua hang tap hoa", 1.020),
    ("HTX", "Hop tac xa",    0.960),
]

# Survey periods do NOT line up with calendar months: May has two, June one,
# July two. Anything that assumes one period per month breaks here.
PERIODS = [
    ("2026-05-K1", "2026-05-08"),
    ("2026-05-K2", "2026-05-22"),
    ("2026-06-K1", "2026-06-10"),
    ("2026-07-K1", "2026-07-09"),
    ("2026-07-K2", "2026-07-24"),
]

# ── Situations planted on purpose, addressed by exact coordinates so the
#    expected outcome is predictable and can be asserted against ──────────

# Every point in this group is unusable. The group must VANISH from the
# report rather than appear as zero — a warehouse that invents a number here
# is worse than one that admits the gap.
ALL_POINTS_BAD = ("QLG-DUONG", "QLG-TLU", "2026-06-K1")

# Only one outlet was surveyed. The average is still computable but rests on
# a single observation, and survey_points is what tells the reader that.
SINGLE_POINT = ("QLG-CAT-VANG", "QLG-MHAO", "2026-07-K1")

# The classic clerical slip: price typed in dong instead of thousand dong.
# One point out of four or five, so the group survives — the demo is that the
# outlier is dropped and the remaining points still produce a sound average.
UNIT_SLIP = ("QLG-GAO-TE", "QLG-VLAM", "2026-05-K1")

# Flipped by /admin/schema-drift to exercise the schema contract check.
SCHEMA_MODE = {"mode": "normal"}


def _build_dataset():
    """Fixed seed, so the same run always produces the same bytes."""
    rnd = random.Random(20260812)
    rows = []
    row_id = 0

    for period_idx, (period, survey_date) in enumerate(PERIODS):
        base_time = datetime.fromisoformat(survey_date).replace(tzinfo=TZ) \
            + timedelta(hours=8)

        for item_code, item_name, uom, base_price in ITEMS:
            for area in AREAS:
                # Phu Cu skipped one period entirely — a gap the report has to
                # show rather than silently render as zero.
                if period == "2026-06-K1" and area == "QLG-PCU":
                    continue

                coords = (item_code, area, period)

                if coords == SINGLE_POINT:
                    outlets = OUTLET_KINDS[:1]
                else:
                    outlets = OUTLET_KINDS[:rnd.choice([3, 4, 5])]

                # Price level for this commodity in this district this period,
                # before per-outlet variation.
                local_level = base_price * (1 + 0.012 * period_idx) \
                    * (1 + rnd.uniform(-0.04, 0.04))

                for kind_idx, (kind, kind_name, kind_factor) in enumerate(outlets):
                    row_id += 1

                    price = local_level * kind_factor * (1 + rnd.uniform(-0.012, 0.012))
                    price = round(price, -2 if base_price > 10_000 else 0)
                    area_code = area

                    # ── Defects ────────────────────────────────────────────
                    if coords == ALL_POINTS_BAD:
                        price = -price                 # nothing usable here
                    elif coords == UNIT_SLIP and kind_idx == 1:
                        price = price * 1000           # dong vs thousand dong
                    else:
                        if row_id % 211 == 0:
                            price = price * 1000
                        if row_id % 313 == 0:
                            price = -price
                        if row_id % 137 == 0:
                            area_code = None           # which district?

                    rows.append({
                        "id": row_id,
                        "itemCode": item_code,
                        "itemName": item_name,
                        "areaCode": area_code,
                        "outletCode": None if area_code is None
                                      else f"{area_code[4:]}-{kind}-{kind_idx + 1:02d}",
                        "outletName": kind_name,
                        "periodCode": period,
                        "surveyDate": survey_date,
                        "uom": uom,
                        "unitPrice": price,
                        "lastModified": (base_time
                                         + timedelta(minutes=row_id % 600)).isoformat(),
                    })

        # The same outlet reported twice, the correction arriving later. Only
        # a deduplication that keeps the newest row survives this.
        for original in rows[-4:-2]:
            row_id += 1
            correction = dict(original)
            correction["id"] = row_id
            correction["lastModified"] = (base_time + timedelta(hours=20)).isoformat()
            rows.append(correction)

        # A provincial subtotal mixed in with the detail rows. SUM over the
        # raw feed double-counts unless this is filtered out.
        row_id += 1
        rows.append({
            "id": row_id,
            "itemCode": "QLG-GAO-TE",
            "itemName": "TONG CONG gao te (toan tinh)",
            "areaCode": "QLG-TONG",
            "outletCode": "TONG",
            "outletName": "Tong hop toan tinh",
            "periodCode": period,
            "surveyDate": survey_date,
            "uom": "kg",
            "unitPrice": sum(
                r["unitPrice"] for r in rows
                if r["periodCode"] == period
                and r["itemCode"] == "QLG-GAO-TE"
                and r["areaCode"] not in (None, "QLG-TONG")
            ),
            "lastModified": (base_time + timedelta(hours=22)).isoformat(),
        })

    rows.sort(key=lambda r: r["lastModified"])
    return rows


DATA = _build_dataset()


def _apply_drift(row):
    """Simulate the source quietly changing its schema."""
    mode = SCHEMA_MODE["mode"]

    if mode == "break":
        # Breaking change: unitPrice turns from a number into a formatted
        # string. The pipeline must STOP, not coerce and carry on — coercing
        # "16.085.000" yields 16.085, wrong by a factor of a million.
        out = dict(row)
        price = row["unitPrice"]
        out["unitPrice"] = (
            f"{price:,.0f}".replace(",", ".") if price is not None else None
        )
        return out

    if mode == "add":
        # Backward-compatible change: a new column appears. Accept it, record
        # it, but do not let it reach a report until someone asks for it.
        out = dict(row)
        out["categoryCode"] = "N" + row["itemCode"][4:7]
        return out

    return row


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _json(self, code, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        print("%s - %s" % (self.address_string(), fmt % args), flush=True)

    def do_GET(self):
        url = urlparse(self.path)
        query = parse_qs(url.query)

        if url.path == "/health":
            return self._json(200, {
                "status": "ok",
                "rowCount": len(DATA),
                "schemaMode": SCHEMA_MODE["mode"],
            })

        if url.path == "/api/periods":
            return self._json(200, {"items": [
                {"periodCode": code, "surveyDate": date}
                for code, date in PERIODS
            ]})

        if url.path == "/api/prices":
            since = query.get("updatedSince", [None])[0]
            period = query.get("period", [None])[0]
            page = int(query.get("page", ["1"])[0])
            page_size = min(int(query.get("pageSize", ["500"])[0]), 2000)

            rows = DATA
            if since:
                normalised = re.sub(r"Z$", "+00:00", since)
                try:
                    cutoff = datetime.fromisoformat(normalised)
                except ValueError:
                    return self._json(400, {"error": "updatedSince is not valid ISO-8601"})
                if cutoff.tzinfo is None:
                    cutoff = cutoff.replace(tzinfo=TZ)
                rows = [r for r in rows
                        if datetime.fromisoformat(r["lastModified"]) > cutoff]
            if period:
                rows = [r for r in rows if r["periodCode"] == period]

            start = (page - 1) * page_size
            window = [_apply_drift(r) for r in rows[start:start + page_size]]
            has_more = start + page_size < len(rows)
            return self._json(200, {
                "items": window,
                "page": page,
                "pageSize": page_size,
                "total": len(rows),
                "nextPage": page + 1 if has_more else None,
            })

        self._json(404, {"error": "not found"})

    def do_POST(self):
        url = urlparse(self.path)
        if url.path == "/admin/schema-drift":
            mode = parse_qs(url.query).get("mode", ["reset"])[0]
            if mode not in ("break", "add", "reset", "normal"):
                return self._json(400, {"error": "mode must be break|add|reset"})
            SCHEMA_MODE["mode"] = "normal" if mode == "reset" else mode
            return self._json(200, {"schemaMode": SCHEMA_MODE["mode"]})
        self._json(404, {"error": "not found"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    print(f"mock QL Gia: {len(DATA)} survey points, {len(PERIODS)} periods, port {port}",
          flush=True)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
