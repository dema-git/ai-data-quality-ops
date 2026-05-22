FROM python:3.11-slim

WORKDIR /app

COPY services/fastapi_app/requirements.txt ./requirements.txt


RUN pip install --no-cache-dir -r requirements.txt

COPY services/fastapi_app /app
COPY services/medallion_models /app/services/medallion_models
COPY services/faker /app/services/faker
COPY services/kafka /app/services/kafka

CMD python models.py && uvicorn main:app --host 0.0.0.0 --port 8000
