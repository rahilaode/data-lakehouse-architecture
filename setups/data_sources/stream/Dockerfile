FROM python:3.10

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


COPY weather-data-generator.py .

CMD ["python3", "weather-data-generator.py"]