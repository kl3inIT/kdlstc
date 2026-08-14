"""
Bài 1 — DAG đầu tiên: hiểu vòng đời một DAG trong Airflow 3.x

Mục tiêu học:
- Thấy DAG xuất hiện trên UI -> unpause -> trigger -> đọc log.
- Hiểu quan hệ: DAG -> Task -> Operator, và scheduler chạy chúng ra sao.

Khái niệm chạm tới: @dag, @task, BashOperator, schedule, catchup,
logical date ({{ ds }}), dependency bằng toán tử >>.
"""
from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator


@dag(
    dag_id="bai01_hello_airflow",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",   # cron rút gọn: chạy mỗi ngày. Đổi thành None để chỉ chạy tay.
    catchup=False,       # Airflow 3.x mặc định False -> KHÔNG chạy bù các ngày quá khứ.
    tags=["hoc-airflow"],
)
def hello_airflow():
    # Task kiểu Bash: chạy 1 lệnh shell BÊN TRONG worker container.
    # {{ ds }} là Jinja template -> logical date của lần chạy (YYYY-MM-DD).
    say_hello = BashOperator(
        task_id="say_hello",
        bash_command="echo 'Xin chao tu Airflow — logical date = {{ ds }}'",
    )

    # Task kiểu Python (TaskFlow): 1 hàm Python = 1 task.
    @task
    def print_context():
        print("Task Python dang chay trong worker.")
        return 42  # giá trị này được lưu vào XCom (bài 2 sẽ dùng)

    # Khai báo phụ thuộc: say_hello chạy XONG rồi mới tới print_context.
    say_hello >> print_context()


# Bắt buộc: gọi hàm @dag ở top-level để Airflow "đăng ký" DAG.
hello_airflow()
