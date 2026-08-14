"""
Bài 3 — Phụ thuộc, retry, rẽ nhánh & xử lý lỗi

Mục tiêu học:
- default_args: cấu hình retry cho mọi task trong DAG.
- @task.branch: rẽ nhánh theo điều kiện runtime.
- trigger_rule: điều khiển khi nào task "gộp nhánh" được chạy.

Đây là nền cho pipeline thật: nguồn có dữ liệu thì xử lý, không thì bỏ qua,
và tự động thử lại khi lỗi tạm thời (mạng, DB bận...).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator


@dag(
    dag_id="bai03_dependencies_retries",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 2,                        # mỗi task thử lại tối đa 2 lần khi fail
        "retry_delay": timedelta(seconds=30),
    },
    tags=["hoc-airflow"],
)
def dependencies_retries():
    start = EmptyOperator(task_id="start")

    @task
    def check_source() -> int:
        # Giả lập đếm số bản ghi ở nguồn. Đổi số này để thử 2 nhánh.
        return 120

    @task.branch
    def route(count: int) -> str:
        # Trả về task_id của nhánh sẽ CHẠY; nhánh còn lại bị "skip".
        return "process_data" if count > 0 else "skip_empty"

    @task
    def process_data() -> None:
        # Da "sua loi DB" -> gio task chay OK.
        print("DB da on -> xu ly du lieu thanh cong")

    skip_empty = EmptyOperator(task_id="skip_empty")

    # `join` cần chạy dù nhánh nào thắng, miễn là không có task nào FAIL.
    join = EmptyOperator(
        task_id="join",
        trigger_rule="none_failed_min_one_success",
    )

    count = check_source()
    start >> count
    route(count) >> [process_data(), skip_empty] >> join


dependencies_retries()
