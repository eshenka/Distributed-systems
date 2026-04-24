from flask import Flask, request, jsonify
from lordkey import LordKey
import socket
import json
import os
import hashlib

app = Flask(__name__)

WORKER_ID = socket.gethostname()
# ALPHABET = json.load(open('/config.json'))['alphabet']
ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789"

@app.route("/process", methods=["POST"])
def process():
    print(os.getcwd(), flush=True)

    data = request.get_json()

    if "word" not in data:
        return jsonify({"error": "word is required"}), 400
    if "idx_start" not in data:
        return jsonify({"error": "idx_start is required"}), 400
    if "idx_end" not in data:
        return jsonify({"error": "idx_end is required"}), 400

    word = data["word"]
    idx_start = int(data["idx_start"])
    idx_end = int(data["idx_end"])

    ret_words = []

    # print(f"Worker {WORKER_ID} received word: {word}")

    lk = LordKey(alphabet=ALPHABET)

    for i in range(idx_start, idx_end):
        gen_word = lk.get_key_by_id(i)
        if word == hashlib.md5(gen_word.encode('utf-8')).hexdigest():
            ret_words.append(gen_word)

    return jsonify({
        "status": "ok",
        "worker": WORKER_ID,
        "received_word": ret_words
    })


@app.route("/")
def health():
    return {"status": "worker running", "worker": WORKER_ID}


app.run(host="0.0.0.0", port=9000)