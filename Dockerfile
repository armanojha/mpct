FROM mcr.microsoft.com/playwright/python:v1.60.0-jammy

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1
ENV ENVIRONMENT=production
ENV TZ=Asia/Kolkata
ENV PORT=8080

EXPOSE 8080

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]
