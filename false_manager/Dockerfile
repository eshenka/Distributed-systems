FROM python:3.11-slim

WORKDIR /manager

COPY app.py .
COPY config.json .

RUN pip install flask requests

EXPOSE 8000

CMD ["python", "app.py"]