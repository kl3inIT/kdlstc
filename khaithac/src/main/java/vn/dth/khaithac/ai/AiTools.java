package vn.dth.khaithac.ai;

import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import vn.dth.khaithac.baocao.Recipe;
import vn.dth.khaithac.baocao.RecipeEngine;
import vn.dth.khaithac.baocao.RecipeRegistry;
import vn.dth.khaithac.nguon.KetQuaNguon;
import vn.dth.khaithac.nguon.NguonClient;
import vn.dth.khaithac.nguon.NguonLoiException;

import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Tool cho agent — MỖI REQUEST một instance, token của user bake sẵn vào
 * constructor (qua AiToolsFactory). Không dùng SecurityContextHolder vì khi
 * streaming, tool chạy trên thread reactor không còn SecurityContext.
 * Token relay xuống Jmix nên quyền xuyên suốt: agent mù như user của nó.
 *
 * Quy ước trả về (agent đọc được):
 *  - {"thieu_quyen": true, "nguon": "..."} → user không có quyền nguồn đó,
 *    agent PHẢI nói rõ, KHÔNG được suy đoán số bị thiếu.
 *  - {"loi": "..."} → hệ thống lỗi — báo lỗi, không được coi là thiếu quyền.
 */
public class AiTools {

    private static final Set<String> LOAI_THU = Set.of("dia_ban", "co_quan_thu", "nguon_thu", "tieu_muc", "sac_thue");

    private final NguonClient nguonClient;
    private final RecipeRegistry recipeRegistry;
    private final RecipeEngine recipeEngine;
    private final String token;

    AiTools(NguonClient nguonClient, RecipeRegistry recipeRegistry,
            RecipeEngine recipeEngine, String token) {
        this.nguonClient = nguonClient;
        this.recipeRegistry = recipeRegistry;
        this.recipeEngine = recipeEngine;
        this.token = token;
    }

    @Tool(description = "Tổng hợp SỐ THU ngân sách theo một chiều. Trả số đo thô (đồng): thuc_hien, so_luong_ct, so_nguoi_nop. Muốn tỷ lệ hoàn thành thì gọi thêm thu_du_toan rồi tự chia.")
    public Object thu_tong_hop(
            @ToolParam(description = "Chiều nhóm: dia_ban | co_quan_thu | nguon_thu | tieu_muc | ky_thoi_gian") String group_by,
            @ToolParam(description = "Từ ngày, dạng yyyy-MM-dd") String tu_ngay,
            @ToolParam(description = "Đến ngày, dạng yyyy-MM-dd") String den_ngay,
            @ToolParam(description = "Lọc theo mã địa bàn (vd HY03), bỏ trống nếu không lọc", required = false) String ma_dia_ban) {
        String q = "/rest/thu/tong-hop?group_by=" + group_by + "&tu_ngay=" + tu_ngay + "&den_ngay=" + den_ngay
                + (blank(ma_dia_ban) ? "" : "&ma_dia_ban=" + ma_dia_ban);
        return goi("THU", q);
    }

    @Tool(description = "Dự toán THU năm theo chiều (dia_ban | co_quan_thu | nguon_thu). Trả du_toan (đồng) từng khóa.")
    public Object thu_du_toan(
            @ToolParam(description = "Năm, vd 2026") int nam,
            @ToolParam(description = "Chiều: dia_ban | co_quan_thu | nguon_thu") String group_by) {
        return goi("THU", "/rest/thu/du-toan?nam=" + nam + "&group_by=" + group_by);
    }

    @Tool(description = "Tổng hợp SỐ CHI ngân sách theo một chiều. Trả số đo thô (đồng): thuc_chi, du_tam_ung, so_luong_ct. Lũy kế chi = thuc_chi + du_tam_ung.")
    public Object chi_tong_hop(
            @ToolParam(description = "Chiều nhóm: don_vi | dia_ban | linh_vuc | nguon_kp | tieu_muc | ky_thoi_gian") String group_by,
            @ToolParam(description = "Từ ngày yyyy-MM-dd") String tu_ngay,
            @ToolParam(description = "Đến ngày yyyy-MM-dd") String den_ngay,
            @ToolParam(description = "Lọc theo mã địa bàn, bỏ trống nếu không lọc", required = false) String ma_dia_ban) {
        String q = "/rest/chi/tong-hop?group_by=" + group_by + "&tu_ngay=" + tu_ngay + "&den_ngay=" + den_ngay
                + (blank(ma_dia_ban) ? "" : "&ma_dia_ban=" + ma_dia_ban);
        return goi("CHI", q);
    }

    @Tool(description = "Dự toán CHI năm theo chiều (don_vi | dia_ban | linh_vuc). Trả du_toan_dau_nam + dieu_chinh_trong_nam (đồng); dự toán hiện hành = tổng 2 số.")
    public Object chi_du_toan(
            @ToolParam(description = "Năm, vd 2026") int nam,
            @ToolParam(description = "Chiều: don_vi | dia_ban | linh_vuc") String group_by) {
        return goi("CHI", "/rest/chi/du-toan?nam=" + nam + "&group_by=" + group_by);
    }

    @Tool(description = "Tra danh mục mã ↔ tên (dia_ban, co_quan_thu, nguon_thu, tieu_muc, don_vi, linh_vuc, nguon_kp). Dùng khi cần đổi tên địa danh/đơn vị người dùng nói sang mã.")
    public Object danh_muc(
            @ToolParam(description = "Loại danh mục: dia_ban | co_quan_thu | nguon_thu | tieu_muc | don_vi | linh_vuc | nguon_kp") String loai,
            @ToolParam(description = "Từ khóa tìm trong tên, bỏ trống để lấy hết", required = false) String tim) {
        String nguon = LOAI_THU.contains(loai.toLowerCase()) ? "THU" : "CHI";
        return goi(nguon, "/rest/danh-muc/" + loai + (blank(tim) ? "" : "?q=" + tim));
    }

    @Tool(description = "Liệt kê các BIỂU BÁO CÁO dựng được: mã biểu, tên, chiều, cần nguồn nào. Dùng khi người dùng hỏi 'có báo cáo nào', 'xem biểu...'.")
    public Object tra_cuu_bieu() {
        return recipeRegistry.all().stream().map(r -> Map.of(
                "ma_bieu", r.maBieu(),
                "ten_bieu", r.tenBieu(),
                "chieu", r.chieu(),
                "nguon_can", r.nguonCalls().stream().map(Recipe.NguonCall::nguon).distinct().toList()
        )).toList();
    }

    @Tool(description = "DỰNG một biểu báo cáo hoàn chỉnh theo mã biểu (lấy mã từ tra_cuu_bieu). Kết quả gồm cột, dòng, và nếu người dùng thiếu quyền nguồn nào thì có nguon_bi_an + khai_bao — PHẢI đọc khai_bao cho người dùng.")
    public Object dung_bieu(
            @ToolParam(description = "Mã biểu, vd DHTC_CHI_04") String ma_bieu,
            @ToolParam(description = "Từ ngày yyyy-MM-dd") String tu_ngay,
            @ToolParam(description = "Đến ngày yyyy-MM-dd") String den_ngay) {
        Recipe r = recipeRegistry.get(ma_bieu);
        if (r == null) {
            return Map.of("loi", "khong co bieu " + ma_bieu + " — goi tra_cuu_bieu de xem danh sach");
        }
        try {
            return recipeEngine.dungBieu(r, LocalDate.parse(tu_ngay), LocalDate.parse(den_ngay), token);
        } catch (NguonLoiException e) {
            return Map.of("loi", e.getMessage(), "nguon", e.getNguon());
        }
    }

    private Object goi(String nguon, String path) {
        try {
            KetQuaNguon kq = nguonClient.get(nguon, path, token);
            if (kq.trangThai() == KetQuaNguon.TrangThai.THIEU_QUYEN) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("thieu_quyen", true);
                m.put("nguon", nguon);
                m.put("huong_dan", "Nguoi dung KHONG co quyen nguon " + nguon
                        + ". Phai noi ro dieu nay, khong duoc suy doan so lieu bi thieu.");
                return m;
            }
            return Map.of("nguon", nguon, "rows", kq.rows());
        } catch (NguonLoiException e) {
            return Map.of("loi", e.getMessage(), "nguon", e.getNguon(),
                    "huong_dan", "He thong nguon dang loi — bao loi cho nguoi dung, KHONG coi la thieu quyen.");
        }
    }

    private static boolean blank(String s) {
        return s == null || s.isBlank();
    }
}
