FROM apache/airflow:2.8.2

WORKDIR /usr/local/airflow

COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt

ENV PYTHONPATH "${PYTHONPATH}:/usr/local/airflow"