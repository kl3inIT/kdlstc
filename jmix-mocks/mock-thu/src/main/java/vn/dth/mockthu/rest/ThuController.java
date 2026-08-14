package vn.dth.mockthu.rest;

import io.jmix.security.role.RoleGrantedAuthorityUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import vn.dth.mockthu.security.ThuApiRole;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Viên gạch API tổng hợp THU — theo contract docs/API-VIEN-GACH-DRAFT.md.
 * Chỉ trả số đo thô cộng dồn được; tỷ lệ / cùng kỳ / tiến độ do consumer tính.
 *
 * Hợp đồng quyền: thiếu role `thu-api` → 403 kèm body {source: "THU"} để tầng
 * khai thác ẩn nhóm cột nguồn THU (phân biệt với lỗi 5xx = báo lỗi, không ẩn).
 */
@RestController
@RequestMapping("/rest/thu")
public class ThuController {

    private static final Map<String, String> GROUP_COLS = Map.of(
            "co_quan_thu", "MA_CO_QUAN_THU",
            "dia_ban", "MA_DIA_BAN",
            "nguon_thu", "MA_NGUON_THU",
            "tieu_muc", "MA_TIEU_MUC");

    private static final Map<String, String> FILTER_COLS = Map.of(
            "ma_dia_ban", "MA_DIA_BAN",
            "ma_co_quan_thu", "MA_CO_QUAN_THU",
            "ma_nguon_thu", "MA_NGUON_THU",
            "ma_tieu_muc", "MA_TIEU_MUC",
            "ma_chuong", "MA_CHUONG");

    private final JdbcTemplate jdbc;
    private final RoleGrantedAuthorityUtils roleAuthorityUtils;

    public ThuController(JdbcTemplate jdbc, RoleGrantedAuthorityUtils roleAuthorityUtils) {
        this.jdbc = jdbc;
        this.roleAuthorityUtils = roleAuthorityUtils;
    }

    /** tong-hop: quyền đầy đủ HOẶC quyền hẹp thu-tong-hop đều xem được. */
    @GetMapping("/tong-hop")
    public ResponseEntity<?> tongHop(@RequestParam("group_by") String groupBy,
                                     @RequestParam("tu_ngay") String tuNgay,
                                     @RequestParam("den_ngay") String denNgay,
                                     @RequestParam(value = "ma_dia_ban", required = false) String maDiaBan,
                                     @RequestParam(value = "ma_co_quan_thu", required = false) String maCoQuanThu,
                                     @RequestParam(value = "ma_nguon_thu", required = false) String maNguonThu,
                                     @RequestParam(value = "ma_tieu_muc", required = false) String maTieuMuc,
                                     @RequestParam(value = "ma_chuong", required = false) String maChuong) {
        ResponseEntity<?> denied = checkPermission(ThuApiRole.CODE, vn.dth.mockthu.security.ThuTongHopRole.CODE);
        if (denied != null) return denied;

        List<Object> params = new ArrayList<>();
        StringBuilder where = new StringBuilder(" WHERE NGAY >= ? AND NGAY <= ?");
        params.add(LocalDate.parse(tuNgay));
        params.add(LocalDate.parse(denNgay));
        appendFilter(where, params, "ma_dia_ban", maDiaBan);
        appendFilter(where, params, "ma_co_quan_thu", maCoQuanThu);
        appendFilter(where, params, "ma_nguon_thu", maNguonThu);
        appendFilter(where, params, "ma_tieu_muc", maTieuMuc);
        appendFilter(where, params, "ma_chuong", maChuong);

        List<Map<String, Object>> rows;
        if ("ky_thoi_gian".equals(groupBy)) {
            rows = jdbc.query(
                    "SELECT YEAR(NGAY) AS NAM, MONTH(NGAY) AS THANG, SUM(SO_TIEN) AS THUC_HIEN, "
                            + "COUNT(*) AS SO_LUONG_CT, COUNT(DISTINCT MA_SO_THUE) AS SO_NGUOI_NOP "
                            + "FROM THU_GIAO_DICH" + where
                            + " GROUP BY YEAR(NGAY), MONTH(NGAY) ORDER BY NAM, THANG",
                    (rs, i) -> {
                        Map<String, Object> r = new LinkedHashMap<>();
                        r.put("khoa", String.format("%04d-%02d", rs.getInt("NAM"), rs.getInt("THANG")));
                        r.put("ten", null);
                        r.put("thuc_hien", rs.getLong("THUC_HIEN"));
                        r.put("so_luong_ct", rs.getLong("SO_LUONG_CT"));
                        r.put("so_nguoi_nop", rs.getLong("SO_NGUOI_NOP"));
                        return r;
                    }, params.toArray());
        } else {
            String col = GROUP_COLS.get(groupBy);
            if (col == null) {
                return ResponseEntity.badRequest().body(Map.of(
                        "error", "invalid_group_by",
                        "cho_phep", List.of("co_quan_thu", "dia_ban", "nguon_thu", "tieu_muc", "ky_thoi_gian")));
            }
            Map<String, String> tenMap = danhMucTen(groupBy);
            rows = jdbc.query(
                    "SELECT " + col + " AS KHOA, SUM(SO_TIEN) AS THUC_HIEN, "
                            + "COUNT(*) AS SO_LUONG_CT, COUNT(DISTINCT MA_SO_THUE) AS SO_NGUOI_NOP "
                            + "FROM THU_GIAO_DICH" + where
                            + " GROUP BY " + col + " ORDER BY " + col,
                    (rs, i) -> {
                        Map<String, Object> r = new LinkedHashMap<>();
                        String khoa = rs.getString("KHOA");
                        r.put("khoa", khoa);
                        r.put("ten", tenMap.get(khoa));
                        r.put("thuc_hien", rs.getLong("THUC_HIEN"));
                        r.put("so_luong_ct", rs.getLong("SO_LUONG_CT"));
                        r.put("so_nguoi_nop", rs.getLong("SO_NGUOI_NOP"));
                        return r;
                    }, params.toArray());
        }
        return ResponseEntity.ok(Map.of(
                "nguon", "THU",
                "group_by", groupBy,
                "tu_ngay", tuNgay,
                "den_ngay", denNgay,
                "rows", rows));
    }

    /** du-toan: CHỈ quyền đầy đủ — user quyền hẹp bị 403 (ẩn cột dự toán). */
    @GetMapping("/du-toan")
    public ResponseEntity<?> duToan(@RequestParam("nam") int nam,
                                    @RequestParam("group_by") String groupBy) {
        ResponseEntity<?> denied = checkPermission(ThuApiRole.CODE);
        if (denied != null) return denied;
        if (!GROUP_COLS.containsKey(groupBy)) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "invalid_group_by", "cho_phep", GROUP_COLS.keySet()));
        }
        Map<String, String> tenMap = danhMucTen(groupBy);
        List<Map<String, Object>> rows = jdbc.query(
                "SELECT KHOA, DU_TOAN FROM THU_DU_TOAN WHERE NAM = ? AND LOAI_CHIEU = ? ORDER BY KHOA",
                (rs, i) -> {
                    Map<String, Object> r = new LinkedHashMap<>();
                    String khoa = rs.getString("KHOA");
                    r.put("khoa", khoa);
                    r.put("ten", tenMap.get(khoa));
                    r.put("du_toan", rs.getLong("DU_TOAN"));
                    return r;
                }, nam, groupBy.toUpperCase());
        return ResponseEntity.ok(Map.of("nguon", "THU", "nam", nam, "group_by", groupBy, "rows", rows));
    }

    private void appendFilter(StringBuilder where, List<Object> params, String name, String value) {
        if (value != null && !value.isBlank()) {
            where.append(" AND ").append(FILTER_COLS.get(name)).append(" = ?");
            params.add(value);
        }
    }

    private Map<String, String> danhMucTen(String groupBy) {
        Map<String, String> map = new LinkedHashMap<>();
        jdbc.query("SELECT MA, TEN FROM DANH_MUC WHERE LOAI = ?",
                rs -> { map.put(rs.getString("MA"), rs.getString("TEN")); },
                groupBy.toUpperCase());
        return map;
    }

    /** null = được phép (có BẤT KỲ role nào trong danh sách); ngược lại 403. */
    private ResponseEntity<?> checkPermission(String... roleCodes) {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        boolean ok = false;
        if (auth != null) {
            for (String code : roleCodes) {
                String required = roleAuthorityUtils
                        .createResourceRoleGrantedAuthority(code).getAuthority();
                if (auth.getAuthorities().stream().anyMatch(a -> required.equals(a.getAuthority()))) {
                    ok = true;
                    break;
                }
            }
        }
        if (ok) return null;
        return ResponseEntity.status(HttpStatus.FORBIDDEN).body(Map.of(
                "error", "no_permission",
                "source", "THU",
                "required_role", String.join(" | ", roleCodes)));
    }
}
