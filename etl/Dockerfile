FROM python:3.11-slim

WORKDIR /app

RUN pip install pandas psycopg2-binary

COPY etl_script.py .

CMD ["python", "etl_script.py"]
