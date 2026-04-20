FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY gateway ./gateway
COPY join .

ENTRYPOINT ["python3", "main.py"]
