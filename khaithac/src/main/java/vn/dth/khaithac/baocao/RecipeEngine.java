package vn.dth.khaithac.baocao;

import org.springframework.stereotype.Component;
import vn.dth.khaithac.nguon.KetQuaNguon;
import vn.dth.khaithac.nguon.NguonClient;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Dựng biểu từ recipe:
 *  1. Gọi các API viên gạch (403-marker → đánh dấu nguồn bị ẩn, KHÔNG fail).
 *  2. Ẩn lan truyền: cột FIELD mất khi nguồn mất; cột công thức mất khi
 *     bất kỳ arg nào mất (fixpoint — đúng logic chống rò rỉ suy luận).
 *  3. Tính cột công thức trên các dòng, chỉ trả giá trị cột còn hiển thị.
 */
@Component
public class RecipeEngine {

    private final NguonClient nguonClient;

    public RecipeEngine(NguonClient nguonClient) {
        this.nguonClient = nguonClient;
    }

    public Map<String, Object> dungBieu(Recipe recipe, LocalDate tuNgay, LocalDate denNgay, String token) {
        // 1. Goi cac nguon — quyen xet theo TUNG LOI GOI, khong phai ca nguon:
        //    user co the duoc phep 1 vien gach (vd thu/tong-hop) nhung bi cam
        //    vien gach khac cung nguon (vd thu/du-toan).
        Map<String, List<Map<String, Object>>> ketQua = new HashMap<>();
        Set<String> callBiAn = new HashSet<>();
        Set<String> nguonBiAn = new HashSet<>();
        for (Recipe.NguonCall call : recipe.nguonCalls()) {
            String path = call.path()
                    .replace("{tu}", tuNgay.toString())
                    .replace("{den}", denNgay.toString())
                    .replace("{nam}", String.valueOf(denNgay.getYear()));
            KetQuaNguon kq = nguonClient.get(call.nguon(), path, token);
            if (kq.trangThai() == KetQuaNguon.TrangThai.THIEU_QUYEN) {
                callBiAn.add(call.id());
                nguonBiAn.add(call.nguon());
            } else {
                ketQua.put(call.id(), kq.rows());
            }
        }

        // 2. An lan truyen theo do thi cong thuc (theo call bi tu choi)
        Set<String> cotBiAn = new HashSet<>();
        boolean doi = true;
        while (doi) {
            doi = false;
            for (Recipe.CotSpec c : recipe.cot()) {
                if (cotBiAn.contains(c.ten())) continue;
                boolean an;
                if (c.tuCall() != null) {
                    an = callBiAn.contains(c.tuCall());
                } else {
                    an = c.args() != null && c.args().stream().anyMatch(cotBiAn::contains);
                }
                if (an) {
                    cotBiAn.add(c.ten());
                    doi = true;
                }
            }
        }

        // 3. Gom dong theo khoa + tinh cong thuc
        Map<String, Map<String, Object>> dong = new TreeMap<>();
        Map<String, String> tenDong = new HashMap<>();
        for (Recipe.NguonCall call : recipe.nguonCalls()) {
            for (Map<String, Object> r : ketQua.getOrDefault(call.id(), List.of())) {
                String khoa = String.valueOf(r.get("khoa"));
                dong.computeIfAbsent(khoa, k -> new HashMap<>());
                Object ten = r.get("ten");
                if (ten != null) tenDong.putIfAbsent(khoa, String.valueOf(ten));
            }
        }

        double tienDoChuan = Math.round(1000.0 * denNgay.getDayOfYear() / denNgay.lengthOfYear()) / 10.0;

        List<Map<String, Object>> rows = new ArrayList<>();
        for (var e : dong.entrySet()) {
            String khoa = e.getKey();
            Map<String, Double> giaTri = new HashMap<>();
            for (Recipe.CotSpec c : recipe.cot()) {
                if (cotBiAn.contains(c.ten())) continue;
                Double v;
                if (c.tuCall() != null) {
                    v = timGiaTri(ketQua.get(c.tuCall()), khoa, c.field());
                } else {
                    v = tinh(c, giaTri, tienDoChuan);
                }
                if (v != null) giaTri.put(c.ten(), v);
            }
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("khoa", khoa);
            row.put("ten", tenDong.getOrDefault(khoa, khoa));
            Map<String, Object> vals = new LinkedHashMap<>();
            for (Recipe.CotSpec c : recipe.cot()) {
                if (c.laCotPhu() || cotBiAn.contains(c.ten())) continue;
                vals.put(c.ten(), giaTri.get(c.ten()));
            }
            row.put("gia_tri", vals);
            rows.add(row);
        }

        // 4. Ket qua
        List<Map<String, Object>> cotHienThi = new ArrayList<>();
        for (Recipe.CotSpec c : recipe.cot()) {
            if (c.laCotPhu()) continue;
            cotHienThi.add(Map.of("ten", c.ten(), "nhan", c.nhan(),
                    "bi_an", cotBiAn.contains(c.ten())));
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ma_bieu", recipe.maBieu());
        out.put("ten_bieu", recipe.tenBieu());
        out.put("tu_ngay", tuNgay.toString());
        out.put("den_ngay", denNgay.toString());
        out.put("nguon_bi_an", nguonBiAn.stream().sorted().toList());
        out.put("cot", cotHienThi);
        out.put("rows", rows);
        out.put("khai_bao", nguonBiAn.isEmpty() ? null
                : "Bạn thiếu quyền với một phần dữ liệu từ nguồn " + String.join(", ", nguonBiAn.stream().sorted().toList())
                + " — các cột thuộc phần đó và cột tính từ chúng đã bị ẩn.");
        return out;
    }

    private Double timGiaTri(List<Map<String, Object>> rows, String khoa, String field) {
        if (rows == null) return null;
        for (Map<String, Object> r : rows) {
            if (khoa.equals(String.valueOf(r.get("khoa")))) {
                Object v = r.get(field);
                return v instanceof Number n ? n.doubleValue() : null;
            }
        }
        return null;
    }

    private Double tinh(Recipe.CotSpec c, Map<String, Double> giaTri, double tienDoChuan) {
        List<String> a = c.args() == null ? List.of() : c.args();
        return switch (c.op()) {
            case "SUM" -> {
                double s = 0;
                boolean co = false;
                for (String x : a) {
                    Double v = giaTri.get(x);
                    if (v != null) { s += v; co = true; }
                }
                yield co ? s : null;
            }
            case "SUB" -> {
                Double x = giaTri.get(a.get(0)), y = giaTri.get(a.get(1));
                yield (x == null || y == null) ? null : lam(x - y);
            }
            case "RATIO_PCT" -> {
                Double x = giaTri.get(a.get(0)), y = giaTri.get(a.get(1));
                yield (x == null || y == null || y == 0) ? null : lam(100.0 * x / y);
            }
            case "TIEN_DO_CHUAN" -> tienDoChuan;
            default -> null;
        };
    }

    private double lam(double v) {
        return Math.round(v * 10.0) / 10.0;
    }
}
