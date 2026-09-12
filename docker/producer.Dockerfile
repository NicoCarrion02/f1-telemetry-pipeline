FROM python:3.10-slim

WORKDIR /app

# Instalar dependencias necesarias
COPY producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código y credenciales
COPY producer/ ./producer/
COPY secrets/ ./secrets/

ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "producer/main.py"]