# Cơ chế DAG dùng chung

Áp dụng cho mọi lát cắt. Quyết định nền: [0007](../decisions/0007-danh-so-dag-theo-buoc-kien-truc.md),
[0008](../decisions/0008-noi-dag-bang-asset.md), [0006](../decisions/0006-pod-hostnetwork-rieng.md).

## Đặt tên

`<slice>_<NN>[b]_<viec>.py` — số bám **bước kiến trúc**, không bám thứ tự viết.
Một bước chẻ làm nhiều DAG thì DAG thứ hai mang chữ cái: `imate_04b_quality_gate`.

## Nối nhau

Bên sản xuất khai `outlets=[X]`, bên tiêu thụ khai `schedule=[X]`. Danh sách
Asset tập trung trong một tệp, đặt tên theo **bước mà nó kết thúc**.

**Tác vụ bị bỏ qua thì không phát sự kiện.** Dùng đặc tính này để cả chuỗi ngủ
khi không có việc:

```python
@task(outlets=[WORKLIST])
def close_run(info):
    pending = worklist_count("discovered")
    if not pending:
        raise AirflowSkipException("Khong co gi moi va khong con viec cho.")
```

## Sổ cái lượt chạy

Mỗi DAG mở một vé trong `ingestion.runs` ở tác vụ đầu và đóng ở tác vụ cuối.
Vé là sợi chỉ xuyên suốt: mọi thứ ghi ra đều mang `run_id`, nên một con số trên
báo cáo lần ngược được về đúng lượt chạy sinh ra nó.

Trạng thái: `received` → `parsed` → `published`, hoặc rẽ sang `schema_blocked` /
`quality_failed`.

Mở vé phải **idempotent** — dùng `ON CONFLICT (run_id) DO UPDATE`, để chạy lại
một lượt Airflow tái dùng đúng vé cũ thay vì đẻ vé rác.

## Bàn giao dữ liệu — dùng bảng, không dùng XCom

Danh sách hàng nghìn dòng không nhét vừa XCom, và nó đằng nào cũng phải bền qua
các lượt chạy. Pod muốn đẩy XCom còn phải có sidecar và volume chung. Kết quả
báo về qua sổ cái; tác vụ cuối đọc lại vé rồi quyết.

Đây cũng là thứ làm cho việc chẻ thành nhiều DAG gần như không tốn gì: điểm bàn
giao vốn đã phải là một bảng.

## Tác vụ chạm mạng

Chạy trong `KubernetesPodOperator` riêng, không chạy trên worker. Ba điểm dễ sai:

```python
hostnetwork=True,                        # KPO viết liền một từ
dnspolicy="ClusterFirstWithHostNet",     # trả lại DNS cụm cho pod
automount_service_account_token=False,   # pod không cần danh tính cụm
```

Import thư viện Kubernetes phải nằm **bên trong** hàm dựng pod, không ở đầu tệp
— import ở mức module từng làm một DAG vượt hạn 30 giây nạp DagBag.

Pod hostNetwork dùng bộ phân giải tên của node, không biết tên dịch vụ trong
cụm. Tra sẵn địa chỉ ra IP ở phía worker rồi truyền vào pod.

## Cầu chì

Mọi vòng lặp quét nguồn phải có trần cứng. Nếu logic so sánh hỏng, mọi bản ghi
trông như mới — thà hỏng to tiếng một lần còn hơn quét lại toàn bộ mỗi 10 phút
mãi mãi. Chạm trần thì **phải báo**, không được dừng lặng lẽ.
