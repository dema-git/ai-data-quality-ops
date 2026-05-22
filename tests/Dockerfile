FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r services/fastapi_app/requirements.txt

RUN mkdir -p /logs

CMD ["bash", "scripts/ci-tests.sh"]