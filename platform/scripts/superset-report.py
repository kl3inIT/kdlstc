# -*- coding: utf-8 -*-
"""
Build the "van ban theo ngay" report in Superset via REST API.

LUU Y sau khi bat SSO (AUTH_OAUTH): endpoint /api/v1/security/login voi
provider "db" khong con hoat dong — script nay chi chay duoc truoc khi bat
SSO, hoac sau nay qua mot service account Keycloak (direct grant) khi can
tu dong hoa lai. Dashboard da tao van nguyen ven, khong phu thuoc script.

Idempotent: looks up existing objects by name before creating.
Steps: login -> database connection (imate_reader, read-only)
       -> virtual dataset (fact + 3 dims flattened)
       -> 4 charts -> dashboard with 3 native filters.
"""
import io
import json
import sys
import uuid

import requests

BASE = "http://bi-stc.10.123.123.194.nip.io"
import os
ADMIN_PW = sys.argv[1] if len(sys.argv) > 1 else os.environ["SUPERSET_ADMIN_PW"]
READER_PW = sys.argv[2] if len(sys.argv) > 2 else os.environ["IMATE_READER_PW"]

out = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", newline="")
p = lambda *a: print(*a, file=out, flush=True)

S = requests.Session()

# ── auth: JWT + CSRF + session cookie, all three needed for POSTs ────────
r = S.post(f"{BASE}/api/v1/security/login", json={
    "username": "admin", "password": ADMIN_PW,
    "provider": "db", "refresh": True}, timeout=30)
r.raise_for_status()
TOKEN = r.json()["access_token"]
H = {"Authorization": f"Bearer {TOKEN}", "Referer": BASE}
r = S.get(f"{BASE}/api/v1/security/csrf_token/", headers=H, timeout=30)
r.raise_for_status()
H["X-CSRFToken"] = r.json()["result"]
p("dang nhap: ok")


def find(endpoint, name_col, name):
    q = json.dumps({"filters": [{"col": name_col, "opr": "eq", "value": name}]})
    r = S.get(f"{BASE}/api/v1/{endpoint}/?q={q}", headers=H, timeout=30)
    r.raise_for_status()
    rows = r.json()["result"]
    return rows[0]["id"] if rows else None


# ── 1. database connection ───────────────────────────────────────────────
DB_NAME = "STC iMate (chi doc)"
db_id = find("database", "database_name", DB_NAME)
if not db_id:
    uri = (f"postgresql+psycopg2://imate_reader:{READER_PW}"
           f"@stc-airflow-postgresql.stc-hy-airflow.svc.cluster.local:5432/stc_imate")
    r = S.post(f"{BASE}/api/v1/database/", headers=H, timeout=60, json={
        "database_name": DB_NAME,
        "sqlalchemy_uri": uri,
        "expose_in_sqllab": True,
        "allow_dml": False,
    })
    if r.status_code not in (200, 201):
        p("LOI database:", r.status_code, r.text[:400]); sys.exit(1)
    db_id = r.json()["id"]
p(f"database: id={db_id}")

# ── 2. virtual dataset ───────────────────────────────────────────────────
DS_NAME = "vw_van_ban_den"
SQL = """SELECT f.global_id,
       f.document_no  AS so_ky_hieu,
       d.full_date    AS ngay,
       d.month_key    AS thang_key,
       d.month_label  AS thang,
       k.kind_name    AS loai,
       b.body_name    AS don_vi_gui
  FROM curated.fact_document f
  JOIN curated.dim_date          d ON d.date_key = f.date_key
  JOIN curated.dim_document_kind k ON k.kind_key = f.kind_key
  JOIN curated.dim_issuing_body  b ON b.body_key = f.body_key"""

ds_id = find("dataset", "table_name", DS_NAME)
if not ds_id:
    r = S.post(f"{BASE}/api/v1/dataset/", headers=H, timeout=60, json={
        "database": db_id, "schema": "curated",
        "table_name": DS_NAME, "sql": SQL})
    if r.status_code not in (200, 201):
        p("LOI dataset:", r.status_code, r.text[:400]); sys.exit(1)
    ds_id = r.json()["id"]

# Deterministic columns + one Vietnamese-named metric.
cols = [
    {"column_name": "global_id",  "type": "TEXT", "groupby": False, "filterable": False},
    {"column_name": "so_ky_hieu", "type": "TEXT", "groupby": True, "filterable": True,
     "verbose_name": "Số ký hiệu"},
    {"column_name": "ngay", "type": "DATE", "is_dttm": True, "groupby": True,
     "filterable": True, "verbose_name": "Ngày văn bản"},
    {"column_name": "thang_key", "type": "BIGINT", "groupby": True, "filterable": True},
    {"column_name": "thang", "type": "TEXT", "groupby": True, "filterable": True,
     "verbose_name": "Tháng"},
    {"column_name": "loai", "type": "TEXT", "groupby": True, "filterable": True,
     "verbose_name": "Loại văn bản"},
    {"column_name": "don_vi_gui", "type": "TEXT", "groupby": True, "filterable": True,
     "verbose_name": "Đơn vị gửi"},
]
r = S.put(f"{BASE}/api/v1/dataset/{ds_id}?override_columns=true", headers=H,
          timeout=60, json={
    "columns": cols,
    "metrics": [{"metric_name": "so_van_ban", "expression": "COUNT(*)",
                 "metric_type": "count", "verbose_name": "Số văn bản"}],
})
if r.status_code != 200:
    p("LOI dataset PUT:", r.status_code, r.text[:400]); sys.exit(1)
p(f"dataset: id={ds_id}")

# ── 3. charts ────────────────────────────────────────────────────────────
DATASOURCE = {"datasource": f"{ds_id}__table"}


def chart(name, viz, params, dash_ids=None):
    # "dashboards" ngay trong payload: thieu no thi layout tro toi chart
    # nhung dashboard khong nhan chart la cua minh — man hinh hien
    # "no chart definition associated". Da dinh bay nay mot lan.
    cid = find("chart", "slice_name", name)
    payload = {"slice_name": name, "viz_type": viz,
               "datasource_id": ds_id, "datasource_type": "table",
               "dashboards": dash_ids or [],
               "params": json.dumps({**DATASOURCE, "viz_type": viz, **params})}
    if cid:
        r = S.put(f"{BASE}/api/v1/chart/{cid}", headers=H, timeout=60, json=payload)
    else:
        r = S.post(f"{BASE}/api/v1/chart/", headers=H, timeout=60, json=payload)
        if r.status_code in (200, 201):
            cid = r.json()["id"]
    if r.status_code not in (200, 201):
        p(f"LOI chart {name}:", r.status_code, r.text[:400]); sys.exit(1)
    ru = S.get(f"{BASE}/api/v1/chart/{cid}", headers=H, timeout=30)
    cuuid = ru.json()["result"].get("uuid") or str(uuid.uuid4())
    p(f"chart: {name} id={cid}")
    return cid, cuuid


c_day = chart("Số văn bản theo ngày", "echarts_timeseries_bar", {
    "x_axis": "ngay", "time_grain_sqla": "P1D",
    "metrics": ["so_van_ban"], "row_limit": 10000,
    "x_axis_title": "Ngày", "y_axis_title": "Số văn bản",
    "show_legend": False, "rich_tooltip": True,
})
c_month = chart("Số văn bản theo tháng", "echarts_timeseries_bar", {
    "x_axis": "ngay", "time_grain_sqla": "P1M",
    "metrics": ["so_van_ban"], "row_limit": 500,
    "x_axis_title": "Tháng", "y_axis_title": "Số văn bản",
    "show_legend": False,
})
c_kind = chart("Theo loại văn bản", "pie", {
    "groupby": ["loai"], "metric": "so_van_ban",
    "row_limit": 25, "show_labels_threshold": 2,
    "donut": True, "label_type": "key_value",
})
c_body = chart("Top đơn vị gửi", "echarts_timeseries_bar", {
    "x_axis": "don_vi_gui", "metrics": ["so_van_ban"],
    "row_limit": 15, "x_axis_sort": "so_van_ban", "x_axis_sort_asc": False,
    "show_legend": False, "y_axis_title": "Số văn bản",
})

# ── 4. dashboard with native filters ─────────────────────────────────────
DASH = "Báo cáo văn bản đến — iMate"
dash_id = find("dashboard", "dashboard_title", DASH)


def cbox(key, cid, cuuid, name, width, height, row):
    return {key: {"type": "CHART", "id": key, "children": [],
                  "parents": ["ROOT_ID", "GRID_ID", row],
                  "meta": {"chartId": cid, "uuid": cuuid, "width": width,
                           "height": height, "sliceName": name}}}


position = {
    "DASHBOARD_VERSION_KEY": "v2",
    "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
    "GRID_ID": {"type": "GRID", "id": "GRID_ID",
                "children": ["ROW-1", "ROW-2"], "parents": ["ROOT_ID"]},
    "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER",
                  "meta": {"text": DASH}},
    "ROW-1": {"type": "ROW", "id": "ROW-1", "children": ["CH-DAY"],
              "parents": ["ROOT_ID", "GRID_ID"],
              "meta": {"background": "BACKGROUND_TRANSPARENT"}},
    "ROW-2": {"type": "ROW", "id": "ROW-2",
              "children": ["CH-MONTH", "CH-KIND", "CH-BODY"],
              "parents": ["ROOT_ID", "GRID_ID"],
              "meta": {"background": "BACKGROUND_TRANSPARENT"}},
}
position.update(cbox("CH-DAY", *c_day, "Số văn bản theo ngày", 12, 60, "ROW-1"))
position.update(cbox("CH-MONTH", *c_month, "Số văn bản theo tháng", 4, 55, "ROW-2"))
position.update(cbox("CH-KIND", *c_kind, "Theo loại văn bản", 4, 55, "ROW-2"))
position.update(cbox("CH-BODY", *c_body, "Top đơn vị gửi", 4, 55, "ROW-2"))


def nfilter(fid, name, column):
    return {"id": f"NATIVE_FILTER-{fid}", "name": name,
            "filterType": "filter_select",
            "targets": [{"datasetId": ds_id, "column": {"name": column}}],
            "defaultDataMask": {"extraFormData": {}, "filterState": {},
                                "ownState": {}},
            "cascadeParentIds": [],
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "controlValues": {"enableEmptyFilter": False,
                              "defaultToFirstItem": False, "multiSelect": True,
                              "searchAllOptions": False,
                              "inverseSelection": False},
            "type": "NATIVE_FILTER", "description": ""}


metadata = {
    "native_filter_configuration": [
        nfilter("thang", "Tháng", "thang"),
        nfilter("loai", "Loại văn bản", "loai"),
        nfilter("dvg", "Đơn vị gửi", "don_vi_gui"),
    ],
    "cross_filters_enabled": True,
    "chart_configuration": {},
}

payload = {"dashboard_title": DASH, "slug": "van-ban-den-imate",
           "published": True,
           "position_json": json.dumps(position),
           "json_metadata": json.dumps(metadata)}
if dash_id:
    r = S.put(f"{BASE}/api/v1/dashboard/{dash_id}", headers=H, timeout=60,
              json=payload)
else:
    r = S.post(f"{BASE}/api/v1/dashboard/", headers=H, timeout=60, json=payload)
    if r.status_code in (200, 201):
        dash_id = r.json()["id"]
if r.status_code not in (200, 201):
    p("LOI dashboard:", r.status_code, r.text[:400]); sys.exit(1)
p(f"dashboard: id={dash_id}")

# ── 5. prove it renders: ask the day-chart for data ──────────────────────
r = S.post(f"{BASE}/api/v1/chart/data", headers=H, timeout=90, json={
    "datasource": {"id": ds_id, "type": "table"},
    "queries": [{"columns": [{"columnType": "BASE_AXIS", "label": "ngay",
                              "sqlExpression": "ngay",
                              "timeGrain": "P1M"}],
                 "metrics": ["so_van_ban"], "row_limit": 500}],
    "result_format": "json", "result_type": "full",
})
if r.status_code == 200:
    data = r.json()["result"][0]["data"]
    total = sum(row.get("so_van_ban", 0) for row in data)
    p(f"kiem chung du lieu: {len(data)} thang, tong {total} van ban")
else:
    p("LOI chart/data:", r.status_code, r.text[:300])

p(f"\nDASHBOARD: {BASE}/superset/dashboard/van-ban-den-imate/")
