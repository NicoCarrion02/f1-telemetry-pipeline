FROM python:3.10-slim

WORKDIR /app

# Instalar dependencias necesarias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código del proyecto necesario para el orquestador
COPY config/ ./config/
COPY data/ ./data/
COPY jobs/ ./jobs/
COPY producer/ ./producer/

ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "jobs/seed.py"]