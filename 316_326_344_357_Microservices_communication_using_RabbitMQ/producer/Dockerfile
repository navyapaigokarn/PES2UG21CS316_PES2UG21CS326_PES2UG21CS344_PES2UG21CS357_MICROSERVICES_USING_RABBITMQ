FROM python:3.11.2

WORKDIR /app

COPY requirements.txt .
COPY static /app/static
COPY templates /app/templates
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000

CMD ["python", "producer.py"]