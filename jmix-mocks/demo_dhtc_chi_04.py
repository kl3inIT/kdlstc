# -*- coding: utf-8 -*-
"""Demo tang khai thac: dung bieu DHTC_CHI_04 (Chi theo dia ban) tu 2 app Jmix.

Chung minh 4 diem cua checklist nghiem thu:
  - 1 bieu ghep tu >=2 API cua 2 app khac nhau (CHI + THU)
  - Cot dan xuat tinh o tang khai thac (luy_ke_chi, ty_le_giai_ngan, can_doi...)
  - 403 tu 1 nguon -> an nhom cot nguon do + AN LAN TRUYEN cot dan xuat
  - Phan biet 403 (thieu quyen, co marker JSON) vs loi khac (fail closed)

Chay:  python demo_dhtc_chi_04.py <username>
       (password lay tu server qua ssh, khong nam trong repo)
"""
import json
import subprocess
import sys
import urllib.parse
import urllib.request
from datetime import date

KC_TOKEN_URL = "https://auth.x2h.com.vn/realms/stc-mock/protocol/openid-connect/token"
CHI_BASE = "http://localhost:8082"
THU_BASE = "http://localhost:8081"
TU_NGAY, DEN_NGAY, NAM = "2026-01-01", "2026-06-30", 2026

# ---- Map cot -> nguon / cong thuc (thu metadata ma bieu that phai co) ----
# nguon: CHI | THU | DAN_XUAT (kem danh sach cot phu thuoc)
COT = [
    ("ma_dia_ban",        "CHI",      None),
    ("ten_dia_ban",       "CHI",      None),
    ("du_toan",           "CHI",      None),               # dau nam + dieu chinh
    ("luy_ke_chi",        "DAN_XUAT", ["thuc_chi", "du_tam_ung"]),
    ("thuc_chi",          "CHI",      None),
    ("du_tam_ung",        "CHI",      None),
    ("so_luong_ct",       "CHI",      None),
    ("ty_le_giai_ngan",   "DAN_XUAT", ["luy_ke_chi", "du_toan"]),
    ("tien_do_chuan",     "DAN_XUAT", []),                 # chi can ngay hien tai
    ("chenh_tien_do",     "DAN_XUAT", ["ty_le_giai_ngan", "tien_do_chuan"]),
    ("thu_dia_ban",       "THU",      None),
    ("can_doi",           "DAN_XUAT", ["thu_dia_ban", "luy_ke_chi"]),
    ("ty_le_tu_can_doi",  "DAN_XUAT", ["thu_dia_ban", "luy_ke_chi"]),
]


def get_password(username):
    out = subprocess.check_output(
        ["ssh", "kdlstc-dev", "cat /home/ubuntu/app/keycloak/stc-mock-test-users.txt"],
        text=True)
    for line in out.splitlines():
        if line.startswith(username + ":"):
            return line.split(":", 1)[1].strip()
    raise SystemExit("khong tim thay password cho " + username)


def get_token(username, password):
    data = urllib.parse.urlencode({
        "grant_type": "password", "client_id": "agent-mock",
        "username": username, "password": password}).encode()
    with urllib.request.urlopen(urllib.request.Request(KC_TOKEN_URL, data=data)) as r:
        return json.load(r)["access_token"]


def call_api(token, url):
    """Tra ve (status, body_dict|None). 403 co marker => thieu quyen.
    Loi khac => nem exception (fail closed: KHONG duoc hieu la thieu quyen)."""
    req = urllib.request.Request(url, headers={"Authorization": "Bearer " + token})
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, json.load(r)
    except urllib.error.HTTPError as e:
        if e.code == 403:
            try:
                body = json.load(e)
            except Exception:
                body = None
            if body and body.get("error") == "no_permission":
                return 403, body          # thieu quyen HOP LE (co marker)
        raise                              # moi truong hop khac: loi that
    # ConnectionError (app tat) tu lan len caller -> bao loi, khong an


def main():
    username = sys.argv[1] if len(sys.argv) > 1 else "user-full"
    token = get_token(username, get_password(username))

    nguon_bi_an = []

    # --- Nguon CHI: tong hop + du toan theo dia ban ---
    chi_rows, du_toan_rows = {}, {}
    st, body = call_api(token, f"{CHI_BASE}/rest/chi/tong-hop?group_by=dia_ban&tu_ngay={TU_NGAY}&den_ngay={DEN_NGAY}")
    if st == 403:
        nguon_bi_an.append("CHI")
    else:
        chi_rows = {r["khoa"]: r for r in body["rows"]}
        st2, body2 = call_api(token, f"{CHI_BASE}/rest/chi/du-toan?nam={NAM}&group_by=dia_ban")
        if st2 != 403:
            du_toan_rows = {r["khoa"]: r for r in body2["rows"]}

    # --- Nguon THU: thu theo dia ban (cot thu_dia_ban cua bieu) ---
    thu_rows = {}
    st, body = call_api(token, f"{THU_BASE}/rest/thu/tong-hop?group_by=dia_ban&tu_ngay={TU_NGAY}&den_ngay={DEN_NGAY}")
    if st == 403:
        nguon_bi_an.append("THU")
    else:
        thu_rows = {r["khoa"]: r for r in body["rows"]}

    # --- An lan truyen: cot mat khi nguon mat, de quy qua cong thuc ---
    hidden = set()
    changed = True
    while changed:
        changed = False
        for ten, nguon, deps in COT:
            if ten in hidden:
                continue
            if nguon in nguon_bi_an or (deps and any(d in hidden for d in deps)):
                hidden.add(ten)
                changed = True

    visible = [c[0] for c in COT if c[0] not in hidden]

    # --- Tinh cot dan xuat + in bieu ---
    ngay_chot = date.fromisoformat(DEN_NGAY)
    tien_do_chuan = round(100.0 * ngay_chot.timetuple().tm_yday / 365, 1)

    print(f"\nDHTC_CHI_04 — Bao cao chi theo dia ban | Ky: {TU_NGAY} -> {DEN_NGAY} | User: {username}")
    if nguon_bi_an:
        print(f"(!) NGUON BI AN do thieu quyen: {', '.join(nguon_bi_an)}"
              f" -> an cac cot: {', '.join(sorted(hidden))}")
    print("-" * 20 + "+" + "-" * (14 * max(1, len(visible) - 2)))
    print(f"{'Dia ban':<20}|" + "".join(f"{c:>14}" for c in visible if c not in ("ma_dia_ban", "ten_dia_ban")))

    khoas = sorted(set(chi_rows) | set(thu_rows))
    for khoa in khoas:
        chi = chi_rows.get(khoa, {})
        dt = du_toan_rows.get(khoa, {})
        thu = thu_rows.get(khoa, {})
        v = {}
        v["thuc_chi"] = chi.get("thuc_chi")
        v["du_tam_ung"] = chi.get("du_tam_ung")
        v["so_luong_ct"] = chi.get("so_luong_ct")
        v["du_toan"] = (dt.get("du_toan_dau_nam", 0) + dt.get("dieu_chinh_trong_nam", 0)) or None
        v["luy_ke_chi"] = (v["thuc_chi"] or 0) + (v["du_tam_ung"] or 0) if chi else None
        v["ty_le_giai_ngan"] = round(100.0 * v["luy_ke_chi"] / v["du_toan"], 1) if v.get("luy_ke_chi") is not None and v.get("du_toan") else None
        v["tien_do_chuan"] = tien_do_chuan
        v["chenh_tien_do"] = round(v["ty_le_giai_ngan"] - tien_do_chuan, 1) if v.get("ty_le_giai_ngan") is not None else None
        v["thu_dia_ban"] = thu.get("thuc_hien")
        v["can_doi"] = (v["thu_dia_ban"] - v["luy_ke_chi"]) if v.get("thu_dia_ban") is not None and v.get("luy_ke_chi") is not None else None
        v["ty_le_tu_can_doi"] = round(100.0 * v["thu_dia_ban"] / v["luy_ke_chi"], 1) if v.get("thu_dia_ban") is not None and v.get("luy_ke_chi") else None

        ten = (chi.get("ten") or thu.get("ten") or khoa)[:19]
        cells = []
        for c in visible:
            if c in ("ma_dia_ban", "ten_dia_ban"):
                continue
            val = v.get(c)
            if val is None:
                cells.append(f"{'—':>14}")
            elif isinstance(val, float):
                cells.append(f"{val:>14.1f}")
            else:
                cells.append(f"{val/1_000_000:>12.0f}tr" if abs(val) >= 1_000_000 else f"{val:>14}")
        print(f"{ten:<20}|" + "".join(cells))

    print()
    if nguon_bi_an:
        print(f"AGENT PHAI KHAI BAO: 'Bao cao thieu du lieu nguon {', '.join(nguon_bi_an)}"
              f" do ban khong co quyen; cac cot lien quan da bi an.'")
    else:
        print("Du quyen ca 2 nguon — bieu day du.")


if __name__ == "__main__":
    main()
