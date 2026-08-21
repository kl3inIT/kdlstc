# 0001 — Bronze bất biến, khoá đặt theo mã băm nội dung

**Trạng thái:** Hiệu lực · **Ngày:** 14/08/2026

## Bối cảnh

Bronze phải là bằng chứng gốc để mọi con số trên báo cáo lần ngược về được.
Nhưng nguồn có thể sửa một bản ghi bất cứ lúc nào, và kho phải giữ được cả bản
cũ lẫn bản mới — nếu không thì không trả lời được câu "báo cáo tháng trước dựa
trên dữ liệu nào".

## Quyết định

Khoá của mỗi đối tượng Bronze là `bronze/doc/<globalId>/<sha256[:16]>.json`,
tức **đặt theo nội dung**, không theo thời điểm tải.

Hệ quả trực tiếp: tải lại một bản ghi y hệt sẽ ghi đè lên chính nó (vô hại),
còn bản ghi đã đổi sinh ra **một đối tượng mới nằm cạnh bản cũ**. Bronze do đó
giữ mọi phiên bản từng thấy.

## Phương án đã loại

**Khoá theo thời điểm tải** (`bronze/doc/<globalId>/<timestamp>.json`) — mỗi
lần chạy lại đẻ một bản sao dù nội dung không đổi, và không phân biệt được
"nguồn đã sửa" với "mình chạy lại".

**Ghi đè một khoá duy nhất cho mỗi bản ghi** — rẻ và gọn, nhưng mất lịch sử,
tức mất luôn khả năng phát lại.

## Hệ quả

Phát lại từ Bronze là **khả thi về dữ liệu** — nhưng đến 21/08/2026 vẫn chưa
có mã thực hiện. Xem [../roadmap.md](../roadmap.md), nhóm 1.

Chi phí lưu trữ tăng theo số lần nguồn sửa, không theo số lần chạy — trong
thực tế đây là con số nhỏ.
