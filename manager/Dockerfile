FROM python:3.11-slim

WORKDIR /manager

RUN pip install flask aiohttp flask-swagger-ui requests

COPY app.py .
COPY swagger.yml .
COPY config.json .

EXPOSE 8000

CMD ["python", "app.py"]