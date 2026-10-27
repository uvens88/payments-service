FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

ARG REQUIREMENTS=requirements.txt
COPY requirements.txt requirements-dev.txt ./
RUN pip install --no-cache-dir -r ${REQUIREMENTS}

COPY . .
RUN useradd --system --no-create-home app
USER app

CMD ["uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "8000"]
