FROM python:3.10.13-slim

RUN apt-get update && \
    apt-get install gcc python3-dev musl-dev libpq-dev -y && \
    apt-get clean

RUN pip install --upgrade pip && \
    pip install --upgrade setuptools

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python3", "-u", "main.py"]