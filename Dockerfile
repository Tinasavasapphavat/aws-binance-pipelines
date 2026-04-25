FROM apache/airflow:2.7.1


USER airflow
RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
