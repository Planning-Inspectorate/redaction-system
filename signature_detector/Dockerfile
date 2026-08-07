FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY score.py app.py ./
COPY model/ ./model/

ENV TRANSFORMERS_OFFLINE=1
ENV HF_HUB_OFFLINE=1
ENV AZUREML_MODEL_DIR=/app/model
ENV MODEL_NAME=.

EXPOSE 8080

CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--timeout", "120", "app:app"]
