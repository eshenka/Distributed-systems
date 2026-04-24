FROM python:3.11-slim

WORKDIR /manager

COPY app.py .
COPY swagger.yml .
COPY config.json .

RUN pip install flask aiohttp flask-swagger-ui requests

EXPOSE 8000

CMD ["python", "app.py"]