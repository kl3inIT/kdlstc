package vn.dth.khaithac.baocao;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Recipe = đặc tả máy-đọc-được của 1 biểu: gọi API nào, cột nào từ đâu,
 * công thức gì. Đây chính là "bảng map cột → nguồn + đồ thị công thức"
 * mà metadata 358 biểu còn thiếu — bản thật sẽ sinh từ metadata + hợp đồng API.
 */
public record Recipe(
        @JsonProperty("ma_bieu") String maBieu,
        @JsonProperty("ten_bieu") String tenBieu,
        @JsonProperty("chieu") String chieu,
        @JsonProperty("nguon_calls") List<NguonCall> nguonCalls,
        @JsonProperty("cot") List<CotSpec> cot) {

    /** 1 lời gọi API viên gạch. path chứa placeholder {tu} {den} {nam}. */
    public record NguonCall(
            @JsonProperty("id") String id,
            @JsonProperty("nguon") String nguon,
            @JsonProperty("path") String path) {
    }

    /**
     * 1 cột của biểu.
     *  - Cột FIELD: có tu_call + field (lấy thẳng từ kết quả API).
     *  - Cột công thức: có op + args (args tham chiếu TÊN CỘT đã khai trước đó).
     *  - an=true: cột phụ chỉ để tính, không hiển thị.
     */
    public record CotSpec(
            @JsonProperty("ten") String ten,
            @JsonProperty("nhan") String nhan,
            @JsonProperty("tu_call") String tuCall,
            @JsonProperty("field") String field,
            @JsonProperty("op") String op,
            @JsonProperty("args") List<String> args,
            @JsonProperty("an") Boolean an) {

        public boolean laCotPhu() {
            return Boolean.TRUE.equals(an);
        }
    }
}
