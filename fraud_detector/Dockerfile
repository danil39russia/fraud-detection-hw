FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

RUN mkdir -p /app/logs && chmod -R 777 /app/logs

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "./app/app.py"]
