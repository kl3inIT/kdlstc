package vn.dth.khaithac.nguon;

/** Nguồn lỗi thật (5xx/timeout/không kết nối được) — báo lỗi biểu, không ẩn cột. */
public class NguonLoiException extends RuntimeException {
    private final String nguon;

    public NguonLoiException(String nguon, String message, Throwable cause) {
        super("Nguon " + nguon + ": " + message, cause);
        this.nguon = nguon;
    }

    public String getNguon() {
        return nguon;
    }
}
