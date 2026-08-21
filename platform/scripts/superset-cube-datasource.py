"""
Trỏ Superset sang đọc qua Cube thay vì đọc thẳng bảng.

Vì sao cần script thay vì bấm trên giao diện: sau khi bật SSO, Superset tắt đăng
nhập bằng mật khẩu, nên API quản trị không dùng được từ bên ngoài. Script này
chạy TRONG pod và mượn app context của chính Superset — cùng đường mà lệnh
`superset` của nó đi.

Vì sao đáng làm: trước đây bước 7 hỏi Cube còn dashboard đọc thẳng bảng, tức hai
kênh khai thác vẫn có thể ra hai con số. Sau script này cả hai đi qua đúng một
định nghĩa trong platform/cube/model.

Idempotent theo TÊN: chạy lại thì cập nhật thứ đã có, không đẻ bản sao.
"""

import os
import sys

from superset.app import create_app

app = create_app()

CUBE_DB_NAME = "STC iMate (qua Cube)"
VIEW = "bao_cao_van_ban"

# Cube SQL API bắt buộc gói chỉ tiêu trong MEASURE(). Superset mặc định sinh
# COUNT(*), nên chỉ tiêu phải khai tường minh — và đó cũng chính là điểm: con số
# này do lớp ngữ nghĩa định nghĩa, không do dashboard tự tính.
METRICS = [
    ("so_van_ban", "Số văn bản", "MEASURE(so_van_ban)"),
    ("so_don_vi_gui", "Số đơn vị gửi", "MEASURE(so_don_vi_gui)"),
]

COLUMNS = [
    ("ngay", "TIMESTAMP", "Ngày", True, True),
    ("thang", "STRING", "Tháng", True, False),
    ("thang_key", "BIGINT", "Tháng (số)", True, False),
    ("nam", "BIGINT", "Năm", True, False),
    ("ten_loai", "STRING", "Loại văn bản", True, False),
    ("ma_loai", "STRING", "Mã loại", True, False),
    ("ten_don_vi", "STRING", "Đơn vị gửi", True, False),
    ("ma_don_vi", "STRING", "Mã đơn vị", True, False),
    ("da_xac_nhan", "BOOLEAN", "Đã xác nhận tên", True, False),
    ("so_ky_hieu", "STRING", "Số ký hiệu", True, False),
    ("trang_thai", "STRING", "Trạng thái", True, False),
]


def main():
    with app.app_context():
        from superset import db
        from superset.connectors.sqla.models import (
            SqlaTable, TableColumn, SqlMetric,
        )
        from superset.models.core import Database

        host = os.environ["CUBE_SQL_HOST"]
        port = os.environ.get("CUBE_SQL_PORT", "15432")
        user = os.environ["CUBE_SQL_USER"]
        password = os.environ["CUBE_SQL_PASSWORD"]
        uri = f"postgresql://{user}:{password}@{host}:{port}/db"

        database = (db.session.query(Database)
                    .filter_by(database_name=CUBE_DB_NAME).one_or_none())
        if database is None:
            database = Database(database_name=CUBE_DB_NAME)
            db.session.add(database)
            print(f"tao database: {CUBE_DB_NAME}")
        else:
            print(f"cap nhat database: {CUBE_DB_NAME}")
        database.sqlalchemy_uri = uri
        database.expose_in_sqllab = True
        database.allow_ctas = False
        database.allow_cvas = False
        database.allow_dml = False
        db.session.flush()

        table = (db.session.query(SqlaTable)
                 .filter_by(table_name=VIEW, database_id=database.id)
                 .one_or_none())
        if table is None:
            table = SqlaTable(table_name=VIEW, database=database, schema="public")
            db.session.add(table)
            print(f"tao dataset: {VIEW}")
        else:
            print(f"cap nhat dataset: {VIEW}")
        table.main_dttm_col = "ngay"
        table.description = ("Đọc qua lớp ngữ nghĩa Cube — "
                             "định nghĩa chỉ tiêu ở platform/cube/model")
        db.session.flush()

        have = {c.column_name: c for c in table.columns}
        for name, kind, label, groupby, is_dttm in COLUMNS:
            column = have.get(name)
            if column is None:
                column = TableColumn(column_name=name, table=table)
                db.session.add(column)
            column.type = kind
            column.verbose_name = label
            column.groupby = groupby
            column.filterable = True
            column.is_dttm = is_dttm

        have_metrics = {m.metric_name: m for m in table.metrics}
        for name, label, expression in METRICS:
            metric = have_metrics.get(name)
            if metric is None:
                metric = SqlMetric(metric_name=name, table=table)
                db.session.add(metric)
            metric.verbose_name = label
            metric.expression = expression

        db.session.commit()
        print(f"dataset id = {table.id}, database id = {database.id}")
        return table.id


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
