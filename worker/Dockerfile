FROM python:3.11-slim

WORKDIR /worker

COPY app.py .
COPY config.json .

RUN pip install flask LordKey requests flask-swagger-ui waitress

EXPOSE 9000

CMD ["python", "app.py"]