package vn.dth.mockthu.rest;

import io.jmix.security.role.RoleGrantedAuthorityUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import vn.dth.mockthu.security.ThuApiRole;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Danh mục dùng chung: mã ↔ tên ↔ cây cha-con.
 * GET /rest/danh-muc/{loai}?q=... — loai: dia_ban | co_quan_thu | nguon_thu | tieu_muc
 */
@RestController
@RequestMapping("/rest/danh-muc")
public class DanhMucController {

    private final JdbcTemplate jdbc;
    private final RoleGrantedAuthorityUtils roleAuthorityUtils;

    public DanhMucController(JdbcTemplate jdbc, RoleGrantedAuthorityUtils roleAuthorityUtils) {
        this.jdbc = jdbc;
        this.roleAuthorityUtils = roleAuthorityUtils;
    }

    @GetMapping("/{loai}")
    public ResponseEntity<?> list(@PathVariable String loai,
                                  @RequestParam(value = "q", required = false) String q) {
        // Danh mục (mã ↔ tên) mở cho cả quyền hẹp thu-tong-hop.
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        boolean ok = false;
        if (auth != null) {
            for (String code : new String[]{ThuApiRole.CODE, vn.dth.mockthu.security.ThuTongHopRole.CODE}) {
                String required = roleAuthorityUtils
                        .createResourceRoleGrantedAuthority(code).getAuthority();
                if (auth.getAuthorities().stream().anyMatch(a -> required.equals(a.getAuthority()))) {
                    ok = true;
                    break;
                }
            }
        }
        if (!ok) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body(Map.of(
                    "error", "no_permission", "source", "THU", "required_role", ThuApiRole.CODE));
        }

        String sql = "SELECT MA, TEN, MA_CHA FROM DANH_MUC WHERE LOAI = ?";
        Object[] params = new Object[]{loai.toUpperCase()};
        if (q != null && !q.isBlank()) {
            sql += " AND LOWER(TEN) LIKE ?";
            params = new Object[]{loai.toUpperCase(), "%" + q.toLowerCase() + "%"};
        }
        List<Map<String, Object>> rows = jdbc.query(sql + " ORDER BY MA", (rs, i) -> {
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("ma", rs.getString("MA"));
            r.put("ten", rs.getString("TEN"));
            r.put("ma_cha", rs.getString("MA_CHA"));
            return r;
        }, params);
        return ResponseEntity.ok(Map.of("loai", loai, "rows", rows));
    }
}
