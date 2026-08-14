FROM apache/airflow:3.3.0-python3.13

# ---------------------------------------------------------------------------
# Provider bổ sung (Oracle...) — cài TRƯỚC phần i18n để tận dụng layer cache.
# Pin trong requirements.txt; dùng constraint của Airflow 3.3.0 để không phá
# vỡ dependency của core. Không cài bằng _PIP_ADDITIONAL_REQUIREMENTS vì cách
# đó cài lại mỗi lần container khởi động (Airflow khuyến cáo chỉ dùng khi dev).
# ---------------------------------------------------------------------------
COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.0/constraints-3.13.txt"

USER root

COPY --chown=airflow:root i18n/locales/en /tmp/airflow-i18n-en

RUN set -eux; \
    target="$(python -c 'import airflow, pathlib; print(pathlib.Path(airflow.__file__).parent / "ui" / "dist" / "i18n" / "locales" / "en")')"; \
    test -d "$(dirname "$target")"; \
    rm -rf "$target"; \
    cp -a /tmp/airflow-i18n-en "$target"; \
    chown -R airflow:root "$target"; \
    rm -rf /tmp/airflow-i18n-en

USER airflow
