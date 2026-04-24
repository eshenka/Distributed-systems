from flask import Flask, request, jsonify
from lordkey import LordKey
from waitress import serve
import socket
import json
import os
import hashlib
import threading
import time
import requests
import logging

app = Flask(__name__)

WORKER_ID = socket.gethostname()
MANAGER_URL = os.getenv('MANAGER_URL', 'http://manager:8000')
ALPHABET = json.load(open('config.json'))['alphabet']
WAITING_TIME = int(os.getenv('WAITING_TIME', '1'))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def send_status(manager_url, task_id, local_task_id, status, progress=None):
    try:
        status_data = {
            "task_id": task_id,
            "local_task_id": local_task_id,
            "status": status,
            "timestamp": time.time()
        }
        
        if progress:
            status_data.update(progress)
            
            if "last_index" not in progress and "current_index" in progress:
                status_data["last_index"] = progress["current_index"]
            
            if "found_words" not in progress and "word" in progress:
                status_data["found_words"] = [progress.get("word")]
        
        response = requests.post(
            f"{manager_url}/status",
            json=status_data,
            timeout=2
        )
        
        if response.status_code == 200:
            logger.debug(f"Status sent successfully for worker {local_task_id}")
            return True
        else:
            logger.warning(f"Failed to send status: {response.status_code}")
            return False
            
    except requests.exceptions.ConnectionError:
        logger.error(f"Worker {WORKER_ID} cannot connect to manager at {manager_url}")
        return False
    except requests.exceptions.Timeout:
        logger.error(f"Worker {WORKER_ID} timeout sending status to manager")
        return False
    except Exception as e:
        logger.error(f"Worker {WORKER_ID} failed to send status: {e}")
        return False

@app.route("/internal/api/worker/hash/crack/task", methods=["POST"])
def process():
    data = request.get_json()
    logger.info(f"Worker {WORKER_ID} received process request: {data}")

    required_fields = ["word", "idx_start", "idx_end", "task_id", "local_task_id"]
    for field in required_fields:
        if field not in data:
            logger.error(f"Missing required field: {field}")
            return jsonify({"error": f"{field} is required"}), 400

    word = data["word"]
    idx_start = int(data["idx_start"])
    idx_end = int(data["idx_end"])
    task_id = data["task_id"]
    local_task_id = data["local_task_id"]
    
    last_processed_index = data.get("last_processed_index", idx_start)
    previously_found_words = data.get("found_words", [])
    
    # вынесла ссылку на менеджера в переменную среды
    manager_url = request.headers.get('X-Manager-Url', MANAGER_URL)

    ret_words = list(previously_found_words)
    lk = LordKey(alphabet=ALPHABET)
    
    start_from = max(idx_start, last_processed_index if last_processed_index is not None else idx_start)
    total_iterations = idx_end - start_from
    
    last_status_time = time.time()
    status_interval = WAITING_TIME

    logger.info(f"Worker {WORKER_ID} ({local_task_id}) processing range [{start_from}, {idx_end}) for task {task_id}")
    logger.info(f"Resuming from index {start_from}, already found {len(ret_words)} words")

    current_index = start_from
    
    try:
        for absolute_idx in range(start_from, idx_end):
            current_index = absolute_idx
            current_time = time.time()
            
            if current_time - last_status_time >= status_interval:
                progress = ((absolute_idx - idx_start) / (idx_end - idx_start) * 100) if idx_end > idx_start else 0
                
                status_sent = send_status(
                    manager_url, 
                    task_id, 
                    local_task_id, 
                    "IN_PROGRESS",
                    {
                        "percentage": round(progress, 2), 
                        "current_index": absolute_idx,
                        "last_index": absolute_idx,
                        "found_words": ret_words,
                        "range_start": idx_start,
                        "range_end": idx_end
                    }
                )
                
                if status_sent:
                    logger.debug(f"Status sent: progress {progress:.2f}%")
                
                last_status_time = current_time
            
            gen_word = lk.get_key_by_id(absolute_idx)
            
            if word == hashlib.md5(gen_word.encode('utf-8')).hexdigest():
                ret_words.append(gen_word)
                logger.info(f"Worker {WORKER_ID} found match: {gen_word} at index {absolute_idx}")
                
                send_status(
                    manager_url, 
                    task_id, 
                    local_task_id, 
                    "FOUND", 
                    {
                        "word": gen_word, 
                        "index": absolute_idx,
                        "last_index": absolute_idx,
                        "found_words": ret_words
                    }
                )
        
        final_status_sent = send_status(
            manager_url, 
            task_id, 
            local_task_id, 
            "COMPLETED", 
            {
                "words_count": len(ret_words), # count 
                "found_words": ret_words,
                "last_index": current_index, # delete
                "final_range": f"[{idx_start}, {idx_end})", # delete
                "progress": 100
            }
        )
        
        logger.info(f"Worker {WORKER_ID} completed processing. Found {len(ret_words)} words. Status sent: {final_status_sent}")
        
        return jsonify({
            "status": "ok",
            "worker": WORKER_ID,
            "local_task_id": local_task_id,
            "task_id": task_id,
            "received_word": ret_words,
            "range_processed": f"[{idx_start}, {idx_end})",
            "last_processed_index": current_index,
            "total_checked": total_iterations
        })
        
    except Exception as e:
        logger.error(f"Worker {WORKER_ID} error during processing: {e}")
        
        send_status(
            manager_url, 
            task_id, 
            local_task_id, 
            "ERROR", 
            {
                "error": str(e),
                "last_index": current_index,
                "found_words": ret_words
            }
        )
        
        return jsonify({
            "status": "error",
            "error": str(e),
            "worker": WORKER_ID,
            "task_id": task_id,
            "last_processed_index": current_index,
            "found_words": ret_words
        }), 500

@app.route("/status", methods=["GET"])
def worker_status():
    return jsonify({
        "local_task_id": WORKER_ID,
        "status": "alive",
        "alphabet": ALPHABET,
        "alphabet_length": len(ALPHABET),
        "uptime": time.time() - start_time if 'start_time' in globals() else 0
    })

@app.route("/")
def index():
    return jsonify({
        "status": "worker running", 
        "worker": WORKER_ID,
        "alphabet_length": len(ALPHABET),
        "endpoints": ["/process", "/status"]
    })

start_time = time.time()

if __name__ == "__main__":
    logger.info(f"Starting worker {WORKER_ID} on port 9000")
    logger.info(f"Alphabet: {ALPHABET[:10]}... (length: {len(ALPHABET)})")
    
    # потоки ограничены
    serve(
        app,
        host="0.0.0.0",
        port=9000,
        threads=10,
        connection_limit=10,
        channel_timeout=300
    )