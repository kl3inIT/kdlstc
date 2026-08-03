package vn.dth.mockchi.rest;

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
import vn.dth.mockchi.security.ChiApiRole;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Viên gạch API tổng hợp CHI — theo contract docs/API-VIEN-GACH-DRAFT.md.
 * Measures thô: thuc_chi (không tạm ứng), du_tam_ung, so_luong_ct.
 * 403 kèm {source: "CHI"} để tầng khai thác ẩn nhóm cột nguồn CHI.
 */
@RestController
@RequestMapping("/rest/chi")
public class ChiController {

    private static final Map<String, String> GROUP_COLS = Map.of(
            "don_vi", "MA_DVQHNS",
            "dia_ban", "MA_DIA_BAN",
            "linh_vuc", "MA_LINH_VUC_CHI",
            "nguon_kp", "MA_NGUON_KP",
            "tieu_muc", "MA_TIEU_MUC");

    private static final Map<String, String> FILTER_COLS = Map.of(
            "ma_dvqhns", "MA_DVQHNS",
            "ma_dia_ban", "MA_DIA_BAN",
            "ma_linh_vuc_chi", "MA_LINH_VUC_CHI",
            "ma_nguon_kp", "MA_NGUON_KP",
            "ma_tieu_muc", "MA_TIEU_MUC");

    /** Tên loại danh mục tương ứng group_by (khác nhau ở linh_vuc/nguon_kp/don_vi). */
    private static final Map<String, String> DM_LOAI = Map.of(
            "don_vi", "DON_VI",
            "dia_ban", "DIA_BAN",
            "linh_vuc", "LINH_VUC",
            "nguon_kp", "NGUON_KP",
            "tieu_muc", "TIEU_MUC");

    private final JdbcTemplate jdbc;
    private final RoleGrantedAuthorityUtils roleAuthorityUtils;

    public ChiController(JdbcTemplate jdbc, RoleGrantedAuthorityUtils roleAuthorityUtils) {
        this.jdbc = jdbc;
        this.roleAuthorityUtils = roleAuthorityUtils;
    }

    @GetMapping("/tong-hop")
    public ResponseEntity<?> tongHop(@RequestParam("group_by") String groupBy,
                                     @RequestParam("tu_ngay") String tuNgay,
                                     @RequestParam("den_ngay") String denNgay,
                                     @RequestParam(value = "ma_dvqhns", required = false) String maDvqhns,
                                     @RequestParam(value = "ma_dia_ban", required = false) String maDiaBan,
                                     @RequestParam(value = "ma_linh_vuc_chi", required = false) String maLinhVucChi,
                                     @RequestParam(value = "ma_nguon_kp", required = false) String maNguonKp,
                                     @RequestParam(value = "ma_tieu_muc", required = false) String maTieuMuc) {
        ResponseEntity<?> denied = checkPermission();
        if (denied != null) return denied;

        List<Object> params = new ArrayList<>();
        StringBuilder where = new StringBuilder(" WHERE NGAY >= ? AND NGAY <= ?");
        params.add(LocalDate.parse(tuNgay));
        params.add(LocalDate.parse(denNgay));
        appendFilter(where, params, "ma_dvqhns", maDvqhns);
        appendFilter(where, params, "ma_dia_ban", maDiaBan);
        appendFilter(where, params, "ma_linh_vuc_chi", maLinhVucChi);
        appendFilter(where, params, "ma_nguon_kp", maNguonKp);
        appendFilter(where, params, "ma_tieu_muc", maTieuMuc);

        String measures = "SUM(CASE WHEN LA_TAM_UNG = TRUE THEN 0 ELSE SO_TIEN END) AS THUC_CHI, "
                + "SUM(CASE WHEN LA_TAM_UNG = TRUE THEN SO_TIEN ELSE 0 END) AS DU_TAM_UNG, "
                + "COUNT(*) AS SO_LUONG_CT ";

        List<Map<String, Object>> rows;
        if ("ky_thoi_gian".equals(groupBy)) {
            rows = jdbc.query(
                    "SELECT YEAR(NGAY) AS NAM, MONTH(NGAY) AS THANG, " + measures
                            + "FROM CHI_GIAO_DICH" + where
                            + " GROUP BY YEAR(NGAY), MONTH(NGAY) ORDER BY NAM, THANG",
                    (rs, i) -> {
                        Map<String, Object> r = new LinkedHashMap<>();
                        r.put("khoa", String.format("%04d-%02d", rs.getInt("NAM"), rs.getInt("THANG")));
                        r.put("ten", null);
                        r.put("thuc_chi", rs.getLong("THUC_CHI"));
                        r.put("du_tam_ung", rs.getLong("DU_TAM_UNG"));
                        r.put("so_luong_ct", rs.getLong("SO_LUONG_CT"));
                        return r;
                    }, params.toArray());
        } else {
            String col = GROUP_COLS.get(groupBy);
            if (col == null) {
                return ResponseEntity.badRequest().body(Map.of(
                        "error", "invalid_group_by",
                        "cho_phep", List.of("don_vi", "dia_ban", "linh_vuc", "nguon_kp", "tieu_muc", "ky_thoi_gian")));
            }
            Map<String, String> tenMap = danhMucTen(DM_LOAI.get(groupBy));
            rows = jdbc.query(
                    "SELECT " + col + " AS KHOA, " + measures
                            + "FROM CHI_GIAO_DICH" + where
                            + " GROUP BY " + col + " ORDER BY " + col,
                    (rs, i) -> {
                        Map<String, Object> r = new LinkedHashMap<>();
                        String khoa = rs.getString("KHOA");
                        r.put("khoa", khoa);
                        r.put("ten", tenMap.get(khoa));
                        r.put("thuc_chi", rs.getLong("THUC_CHI"));
                        r.put("du_tam_ung", rs.getLong("DU_TAM_UNG"));
                        r.put("so_luong_ct", rs.getLong("SO_LUONG_CT"));
                        return r;
                    }, params.toArray());
        }
        return ResponseEntity.ok(Map.of(
                "nguon", "CHI",
                "group_by", groupBy,
                "tu_ngay", tuNgay,
                "den_ngay", denNgay,
                "rows", rows));
    }

    @GetMapping("/du-toan")
    public ResponseEntity<?> duToan(@RequestParam("nam") int nam,
                                    @RequestParam("group_by") String groupBy) {
        ResponseEntity<?> denied = checkPermission();
        if (denied != null) return denied;
        if (!GROUP_COLS.containsKey(groupBy)) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "invalid_group_by", "cho_phep", GROUP_COLS.keySet()));
        }
        Map<String, String> tenMap = danhMucTen(DM_LOAI.get(groupBy));
        List<Map<String, Object>> rows = jdbc.query(
                "SELECT KHOA, DU_TOAN_DAU_NAM, DIEU_CHINH_TRONG_NAM FROM CHI_DU_TOAN "
                        + "WHERE NAM = ? AND LOAI_CHIEU = ? ORDER BY KHOA",
                (rs, i) -> {
                    Map<String, Object> r = new LinkedHashMap<>();
                    String khoa = rs.getString("KHOA");
                    r.put("khoa", khoa);
                    r.put("ten", tenMap.get(khoa));
                    r.put("du_toan_dau_nam", rs.getLong("DU_TOAN_DAU_NAM"));
                    r.put("dieu_chinh_trong_nam", rs.getLong("DIEU_CHINH_TRONG_NAM"));
                    return r;
                }, nam, DM_LOAI.get(groupBy));
        return ResponseEntity.ok(Map.of("nguon", "CHI", "nam", nam, "group_by", groupBy, "rows", rows));
    }

    private void appendFilter(StringBuilder where, List<Object> params, String name, String value) {
        if (value != null && !value.isBlank()) {
            where.append(" AND ").append(FILTER_COLS.get(name)).append(" = ?");
            params.add(value);
        }
    }

    private Map<String, String> danhMucTen(String loai) {
        Map<String, String> map = new LinkedHashMap<>();
        jdbc.query("SELECT MA, TEN FROM DANH_MUC WHERE LOAI = ?",
                rs -> { map.put(rs.getString("MA"), rs.getString("TEN")); },
                loai);
        return map;
    }

    /** null = được phép; ngược lại trả sẵn 403. */
    private ResponseEntity<?> checkPermission() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        String required = roleAuthorityUtils
                .createResourceRoleGrantedAuthority(ChiApiRole.CODE).getAuthority();
        boolean ok = auth != null && auth.getAuthorities().stream()
                .anyMatch(a -> required.equals(a.getAuthority()));
        if (ok) return null;
        return ResponseEntity.status(HttpStatus.FORBIDDEN).body(Map.of(
                "error", "no_permission",
                "source", "CHI",
                "required_role", ChiApiRole.CODE));
    }
}
