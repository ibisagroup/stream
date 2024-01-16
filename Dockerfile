FROM python:3.10.13-slim

RUN apt-get update && \
    apt-get install gcc python3-dev musl-dev libpq-dev -y && \
    apt-get clean

RUN pip install --upgrade pip && \
    pip3 install --upgrade setuptools

COPY . .

RUN pip3 install --no-cache-dir -r requirements.txt

RUN ls
RUN ls /home
# RUN ls /code

# CMD ["python3", "-u", "data.py"]
# CMD ["python3", "-u", "data.py"]

# CMD ["sh", "-c", "python3 data.py && python3 api.py"]

CMD ["/bin/bash", "run_both_scripts.sh"]
