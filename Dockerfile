FROM apache/airflow:2.0.2
COPY ./requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --user --no-cache-dir xlrd pandas
