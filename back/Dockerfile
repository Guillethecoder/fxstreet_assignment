from python:latest

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt
COPY utils.py .
COPY main.py .