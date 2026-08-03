"""
Bài 2 — TaskFlow API & truyền dữ liệu giữa các task (XCom)

Mục tiêu học:
- Mô hình extract -> transform -> load bằng @task.
- Giá trị `return` của task này TỰ ĐỘNG đi qua XCom sang task sau,
  không cần gọi xcom_pull() thủ công như Airflow 1.x/2.x.

Đây là "hình dạng" của gần như mọi pipeline kho dữ liệu:
lấy nguồn -> biến đổi -> nạp vào warehouse.
"""
from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="bai02_taskflow_etl",
    start_date=datetime(2026, 1, 1),
    schedule=None,       # chỉ chạy khi bấm Trigger trên UI
    catchup=False,
    tags=["hoc-airflow"],
)
def taskflow_etl():
    @task
    def extract() -> dict[str, float]:
        # Giả lập lấy dữ liệu nguồn. Sau này thay bằng API / truy vấn DB thật.
        return {"HD001": 301.27, "HD002": 433.21, "HD003": 502.22}

    @task
    def transform(orders: dict[str, float]) -> dict[str, float]:
        total = sum(orders.values())
        return {"tong_gia_tri": round(total, 2), "so_hoa_don": len(orders)}

    @task
    def load(summary: dict[str, float]) -> None:
        # Sau này: ghi vào bảng trong kho dữ liệu. Giờ chỉ in ra log để quan sát.
        print(f"Nap vao kho: {summary}")

    # Nối chuỗi: output của hàm này là input của hàm kia
    # -> Airflow tự SUY RA thứ tự phụ thuộc extract -> transform -> load.
    load(transform(extract()))


taskflow_etl()
