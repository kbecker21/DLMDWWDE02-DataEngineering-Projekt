FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY download_eea.py .

ENTRYPOINT ["python", "download_eea.py"]
