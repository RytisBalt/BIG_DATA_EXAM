FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    default-jre \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY Exam.py .

RUN mkdir -p /app/output

CMD ["python", "Exam.py"]