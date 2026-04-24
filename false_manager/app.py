import json
import requests
from flask import Flask, request, jsonify
import os

app = Flask(__name__)

WORKERS_COUNT = int(os.getenv('WORKERS', '3'))
WORKER_PORT = os.getenv('WORKER_PORT', '9000')
ALPHABET = json.load(open('config.json'))['alphabet']

workers = [
    f"http://docker_course-worker-{i}:{WORKER_PORT}/process"
    for i in range(1, WORKERS_COUNT + 1)
]

current_worker = 0

def get_next_worker():
    global current_worker
    worker = workers[current_worker]
    current_worker = (current_worker + 1) % len(workers)
    return worker

@app.route("/task", methods=["POST"])
def task():
    data = request.get_json()

    if "word" not in data:
        return jsonify({"error": "word is required"}), 400
    if "length" not in data: 
        return jsonify({"error": "length is required"}), 400

    word = data["word"]
    length = int(data['length'])

    idx_start = 0
    idx_end = sum(len(ALPHABET)**i for i in range(1, length+1))

    worker_url = get_next_worker()

    try:
        print(f"Sending to: {worker_url}")
        response = requests.post(worker_url, json={"word": word, "idx_start": idx_start, "idx_end": idx_end})

        return jsonify({
            "sent_to": worker_url,
            "worker_response": response.json()
        })

    except requests.exceptions.RequestException as e:
        return jsonify({
            "error": "worker unavailable",
            "details": str(e)
        }), 500

@app.route("/")
def health():
    return {"status": "manager running", "workers": workers}

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)

