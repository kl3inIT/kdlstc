#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sinh dữ liệu mô phỏng tài chính tỉnh (VN) cho 2 nguồn:
- Oracle TMS  : nnt, to_khai, khoan_nop
- Postgres KBNN: don_vi, giao_dich_thu, giao_dich_chi

Độ bẩn seed theo pattern THẬT (bắt chước Olist), KHÔNG random vô nghĩa:
  1. Cùng 1 MST xuất hiện ở 2 nguồn với TÊN VIẾT KHÁC NHAU  -> dedup
  2. khoan_nop ĐẾN SAU to_khai vài ngày                    -> late-arriving
  3. MST định dạng lệch (có/không gạch)                    -> chuẩn hóa
  4. Tổng thu KBNN LỆCH tổng khoản nộp TMS (thiếu 1 khoản) -> đối soát

Chạy: python3 gen_data.py
Sinh ra 2 file: /tmp/tms_data.sql (Oracle), /tmp/kbnn_data.sql (Postgres)
Dùng seed cố định để tái lập (KHÔNG random.random mỗi lần khác).
"""
import random

random.seed(20260716)  # cố định -> chạy lại ra y hệt

# --- Dữ liệu gốc tiếng Việt (nhúng sẵn, không cần Faker) ---
HO = ["Nguyen", "Tran", "Le", "Pham", "Hoang", "Vu", "Dang", "Bui", "Do", "Ngo"]
TEN_DEM = ["Van", "Thi", "Duc", "Minh", "Quang", "Thanh", "Huu", "Cong", "Thu", "Hong"]
TEN = ["An", "Binh", "Cuong", "Dung", "Ha", "Hung", "Lan", "Nam", "Phuong", "Son", "Tuan", "Yen"]
LOAI_HINH = ["Cong ty TNHH", "Cong ty CP", "DNTN", "HKD"]
HUYEN = ["TP Hung Yen", "My Hao", "Van Lam", "Yen My", "Khoai Chau", "Tien Lu", "Kim Dong", "An Thi"]
SAC_THUE = ["GTGT", "TNDN", "TNCN", "MON"]
# Tiểu mục Mục lục NSNN (mã thật) theo sắc thuế
TIEU_MUC = {"GTGT": "1701", "TNDN": "1052", "TNCN": "1001", "MON": "2863"}

def ten_nguoi():
    return f"{random.choice(HO)} {random.choice(TEN_DEM)} {random.choice(TEN)}"

def ten_dn():
    return f"{random.choice(LOAI_HINH)} {random.choice(TEN)} {random.choice(TEN)}"

def mst_gen(i):
    # MST 10 số: 0100000000 + i
    return f"01{i:08d}"

def esc(s):
    return s.replace("'", "''")

# --- Sinh N người nộp thuế ---
N_NNT = 40
nnt_list = []
for i in range(1, N_NNT + 1):
    mst = mst_gen(i)
    ten = ten_dn() if i % 3 else ten_nguoi()
    huyen = random.choice(HUYEN)
    dia_chi = f"{random.randint(1,200)} duong {random.choice(TEN)}, {huyen}, Hung Yen"
    cqt = f"Chi cuc Thue {huyen}"
    nnt_list.append({"mst": mst, "ten": ten, "dia_chi": dia_chi, "cqt": cqt, "huyen": huyen})

# --- Sinh tờ khai + khoản nộp ---
to_khai_list = []
khoan_nop_list = []
tk_id = 1
kn_id = 1
for nnt in nnt_list:
    so_to_khai = random.randint(1, 4)
    for _ in range(so_to_khai):
        sac = random.choice(SAC_THUE)
        thang = random.randint(1, 6)
        ky = f"2026-{thang:02d}"
        so_tien = random.choice([5, 10, 15, 20, 30, 50, 80, 120]) * 1_000_000
        ngay_khai = f"2026-{thang:02d}-{random.randint(18,20)}"
        to_khai_list.append({
            "id": tk_id, "mst": nnt["mst"], "sac": sac, "ky": ky,
            "so_tien": so_tien, "ngay_khai": ngay_khai
        })
        # Khoản nộp: phần lớn nộp, nhưng ĐẾN SAU vài ngày (late-arriving)
        if random.random() > 0.15:  # 85% có nộp
            delay = random.randint(1, 8)
            ngay_nop_thang = thang
            ngay_nop_ngay = 20 + delay
            if ngay_nop_ngay > 28:
                ngay_nop_thang += 1
                ngay_nop_ngay -= 28
            ngay_nop = f"2026-{ngay_nop_thang:02d}-{ngay_nop_ngay:02d}"
            khoan_nop_list.append({
                "id": kn_id, "tk_id": tk_id, "so_tien": so_tien,
                "ngay_nop": ngay_nop, "tieu_muc": TIEU_MUC[sac], "mst": nnt["mst"], "sac": sac
            })
            kn_id += 1
        tk_id += 1

# ============================================================
# FILE 1: Oracle TMS
# ============================================================
with open("/tmp/tms_data.sql", "w", encoding="utf-8") as f:
    f.write("-- Du lieu nguon Thue (Oracle TMS)\n")
    f.write("DELETE FROM tms.khoan_nop;\nDELETE FROM tms.to_khai;\nDELETE FROM tms.nnt;\n")
    for n in nnt_list:
        f.write(
            f"INSERT INTO tms.nnt (mst, ten_nnt, dia_chi, co_quan_thue) VALUES "
            f"('{n['mst']}', '{esc(n['ten'])}', '{esc(n['dia_chi'])}', '{esc(n['cqt'])}');\n"
        )
    for t in to_khai_list:
        f.write(
            f"INSERT INTO tms.to_khai (id, mst, ma_sac_thue, ky_khai, so_tien_ke_khai, ngay_khai) VALUES "
            f"({t['id']}, '{t['mst']}', '{t['sac']}', '{t['ky']}', {t['so_tien']}, DATE '{t['ngay_khai']}');\n"
        )
    for k in khoan_nop_list:
        f.write(
            f"INSERT INTO tms.khoan_nop (id, to_khai_id, so_tien_nop, ngay_nop, ma_tieu_muc) VALUES "
            f"({k['id']}, {k['tk_id']}, {k['so_tien']}, DATE '{k['ngay_nop']}', '{k['tieu_muc']}');\n"
        )
    f.write("COMMIT;\n")
    f.write("SELECT 'nnt' bang, COUNT(*) n FROM tms.nnt "
            "UNION ALL SELECT 'to_khai', COUNT(*) FROM tms.to_khai "
            "UNION ALL SELECT 'khoan_nop', COUNT(*) FROM tms.khoan_nop;\n")
    f.write("EXIT\n")

# ============================================================
# FILE 2: Postgres KBNN
# Nguon Kho bac: giao_dich_thu tham chieu MST tu TMS
# SEED BAN: - cung MST nhung TEN KHAC (dedup)
#           - MST dinh dang lech (gach)
#           - THIEU 1 giao dich thu -> tong LECH (doi soat)
# ============================================================
DON_VI = [
    ("1067001", "Van phong UBND tinh", 1),
    ("1067002", "So Tai chinh", 2),
    ("1067003", "So Giao duc va Dao tao", 2),
    ("1067004", "So Y te", 2),
    ("1067005", "UBND TP Hung Yen", 3),
]
with open("/tmp/kbnn_data.sql", "w", encoding="utf-8") as f:
    f.write("-- Du lieu nguon Kho bac (Postgres KBNN)\n")
    f.write("TRUNCATE giao_dich_chi, giao_dich_thu, don_vi RESTART IDENTITY CASCADE;\n")
    for d in DON_VI:
        f.write(f"INSERT INTO don_vi (ma_dvsdns, ten_dvsdns, cap_ns) VALUES ('{d[0]}', '{esc(d[1])}', {d[2]});\n")
    # giao_dich_thu: doi ung voi khoan_nop cua TMS, nhung:
    #   - bo qua khoan nop cuoi cung (thieu 1) -> tong lech
    #   - ~30% NNT co TEN KHAC (viet tat / dau) va MST co gach
    ten_khac_map = {}
    for idx, k in enumerate(khoan_nop_list):
        if idx == len(khoan_nop_list) - 1:
            continue  # THIEU khoan cuoi -> doi soat se phat hien lech
        # ten NNT o phia kho bac (co the khac)
        nnt = next(n for n in nnt_list if n["mst"] == k["mst"])
        ten_kb = nnt["ten"]
        mst_kb = k["mst"]
        if hash(k["mst"]) % 10 < 3:  # ~30% ten viet khac + MST co gach
            ten_kb = ten_kb.upper()               # viet HOA khac
            mst_kb = f"{k['mst'][:4]}-{k['mst'][4:]}"  # them gach: 0100-000001
        f.write(
            f"INSERT INTO giao_dich_thu (mst, ten_nnt_kb, so_tien, ngay_thu, ma_tieu_muc, ma_sac_thue) VALUES "
            f"('{mst_kb}', '{esc(ten_kb)}', {k['so_tien']}, DATE '{k['ngay_nop']}', '{k['tieu_muc']}', '{k['sac']}');\n"
        )
    # giao_dich_chi: chi ngan sach theo don vi
    ma_muc_chi = ["6001", "6051", "6101", "7001", "7051"]  # tieu muc chi
    gid = 1
    for d in DON_VI:
        for _ in range(random.randint(2, 4)):
            so_tien = random.choice([50, 100, 200, 300, 500]) * 1_000_000
            thang = random.randint(1, 6)
            f.write(
                f"INSERT INTO giao_dich_chi (ma_dvsdns, so_tien, ngay_chi, ma_tieu_muc) VALUES "
                f"('{d[0]}', {so_tien}, DATE '2026-{thang:02d}-{random.randint(1,28):02d}', '{random.choice(ma_muc_chi)}');\n"
            )
            gid += 1
    f.write("SELECT 'don_vi' bang, COUNT(*) n FROM don_vi "
            "UNION ALL SELECT 'giao_dich_thu', COUNT(*) FROM giao_dich_thu "
            "UNION ALL SELECT 'giao_dich_chi', COUNT(*) FROM giao_dich_chi;\n")

# --- Thong ke tom tat de doi chieu ---
tong_khoan_nop = sum(k["so_tien"] for k in khoan_nop_list)
tong_thu_kb = sum(k["so_tien"] for i, k in enumerate(khoan_nop_list) if i != len(khoan_nop_list) - 1)
print(f"NNT: {len(nnt_list)}")
print(f"To khai: {len(to_khai_list)}")
print(f"Khoan nop (TMS): {len(khoan_nop_list)} | tong tien: {tong_khoan_nop:,}")
print(f"Giao dich thu (KBNN): {len(khoan_nop_list)-1} | tong tien: {tong_thu_kb:,}")
print(f"CHENH LECH doi soat (do thieu 1 khoan): {tong_khoan_nop - tong_thu_kb:,}")
