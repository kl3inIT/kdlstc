package vn.dth.khaithac.nguon;

import java.util.List;
import java.util.Map;

/**
 * Kết quả gọi 1 API viên gạch.
 * THIEU_QUYEN chỉ khi 403 kèm marker {error:"no_permission"} — mọi lỗi khác
 * ném NguonLoiException (fail closed: lỗi không bao giờ được hiểu là thiếu quyền).
 */
public record KetQuaNguon(TrangThai trangThai, List<Map<String, Object>> rows) {

    public enum TrangThai { OK, THIEU_QUYEN }

    public static KetQuaNguon ok(List<Map<String, Object>> rows) {
        return new KetQuaNguon(TrangThai.OK, rows);
    }

    public static KetQuaNguon thieuQuyen() {
        return new KetQuaNguon(TrangThai.THIEU_QUYEN, List.of());
    }
}
