FROM python:3.10-slim

WORKDIR /app

# Instalar dependencias necesarias
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copiar el código y credenciales
COPY config/ ./config/
COPY producer/ ./producer/

ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "producer/main.py"]