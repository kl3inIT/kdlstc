# 0003 — Quét mới-nhất-trước, dừng sau ba trang sạch

**Trạng thái:** Hiệu lực · **Ngày:** 14/08/2026

## Bối cảnh

API iMate **không có bộ lọc theo khoảng thời gian**. Tham số `filter[updatedAt]`
chỉ khớp đúng một mốc; thêm tiền tố kiểu "lớn hơn hoặc bằng" thì máy chủ trả
HTTP 400. Không có cách nào hỏi "cho tôi mọi thứ kể từ ngày X".

Thứ duy nhất dùng được là sắp thứ tự. Nhưng nếu một bản ghi bị sửa **ngay giữa
lúc đang quét**, nó đổi chỗ và cả danh sách xê dịch theo.

## Quyết định

Quét theo `sort[updatedAt]=DESC`, dừng sau **ba trang liên tiếp** không có gì
mới hoặc đổi. Cầu chì `MAX_PAGES = 200`.

## Vì sao giảm dần

Bản ghi bị sửa nhảy lên đầu, đẩy các dòng chưa đọc **ra xa** con trỏ — tệ nhất
là đọc lại một dòng đã đọc, và khoá chính loại trùng ngay.

Sắp tăng dần thì đúng sự kiện ấy đẩy các dòng chưa đọc **lại gần** con trỏ, và
vòng quét bước qua mất một dòng. Đọc trùng sửa được; bỏ sót thì không ai biết.

## Vì sao ba trang, không phải một

Nguồn ghi bản ghi theo đợt. Một trang sạch đơn lẻ có thể chỉ là khe hở giữa hai
đợt. Ba trang là 300 bản ghi đệm — đủ chắc, mà chi phí mỗi lượt vẫn chỉ ba lời
gọi HTTP, chạy được 10 phút một lần.

## Phương án đã loại

**Quét đủ 62 trang mỗi lượt** — chắc chắn nhất, nhưng tốn gấp 15 lần và không
mua thêm gì so với ba trang đệm.

**Tin vào một trang sạch** — rẻ nhất, nhưng gặp nguồn ghi theo đợt là sót.

## Hệ quả

Nguồn **xoá** bản ghi thì cách quét này không bao giờ thấy — vì nó dừng sớm.
Điểm mù này được đo bằng chênh lệch giữa số kho giữ và tổng số nguồn báo, ghi
vào bản tóm tắt lượt chạy. Đến 21/08/2026 chưa có ngưỡng cảnh báo.

Nguồn sửa bản ghi mà **quên nâng `updatedAt`** thì cũng không phát hiện được.
Chưa có cơ chế bù.
