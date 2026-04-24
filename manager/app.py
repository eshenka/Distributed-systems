from flask import Flask, request, jsonify, send_from_directory
import aiohttp
import asyncio
import logging
import uuid
import os
import json
import time
import random
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
import threading
from concurrent.futures import ThreadPoolExecutor
from flask_swagger_ui import get_swaggerui_blueprint

app = Flask(__name__)

WORKERS_COUNT = int(os.getenv('WORKERS', '3'))
WORKER_PORT = os.getenv('WORKER_PORT', '9000')
TIMEOUT_WORKER_TIME = int(os.getenv('TIMEOUT_WORKER_TIME', '2'))
WAITING_TIME = int(os.getenv('WAITING_TIME', '1'))
ALPHABET = json.load(open('config.json'))['alphabet']

WORKERS = [
    f"http://docker_course-worker-{i}:{WORKER_PORT}"
    for i in range(1, WORKERS_COUNT + 1)
]

RETRIES_MAX_NUMBER = 2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

task_statuses: Dict[str, Dict[str, Any]] = {}
status_lock = threading.Lock()

executor = ThreadPoolExecutor(max_workers=4)

SWAGGER_URL = '/swagger'
API_URL = '/swagger.yml'

swaggerui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={
        'app_name': "Hash Cracker API",
        'layout': "BaseLayout",
        'deepLinking': True,
        'displayOperationId': False,
        'defaultModelsExpandDepth': -1,
        'docExpansion': 'list',
        'defaultModelExpandDepth': 2,
        'showExtensions': True,
        'showCommonExtensions': True,
        'supportedSubmitMethods': ['get', 'post', 'put', 'delete', 'patch'],
        'validatorUrl': None,
    }
)

app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)

@app.route('/swagger.yml')
def send_swagger():
    return send_from_directory('.', 'swagger.yml')

@app.route('/')
def index():
    return f'''
    <html>
        <head>
            <title>Hash Cracker API</title>
            <style>
                body {{ 
                    font-family: Arial, sans-serif; 
                    margin: 40px; 
                    line-height: 1.6;
                }}
                h1 {{ color: #333; }}
                .endpoint {{ 
                    background: #f4f4f4; 
                    padding: 10px; 
                    border-radius: 5px;
                    margin: 10px 0;
                }}
                .endpoint a {{
                    text-decoration: none;
                    color: #0066cc;
                }}
                .endpoint a:hover {{
                    text-decoration: underline;
                }}
            </style>
        </head>
        <body>
            <h1>Hash Cracker API</h1>
            <p>Добро пожаловать в API распределённой системы взлома хешей!</p>
            
            <div class="endpoint">
                <a href="/swagger">Swagger UI (интерактивная документация)</a>
            </div>
            
            <h2>Основные эндпоинты:</h2>
            <ul>
                <li><strong>POST /api/hash/crack</strong> - создать задачу на взлом</li>
                <li><strong>GET /api/hash/status?requestId={{task_id}}</strong> - проверить статус</li>
            </ul>
            
            <h2>Отладочные эндпоинты:</h2>
            <ul>
                <li><strong>GET /task/{{task_id}}/status</strong> - детальный статус</li>
                <li><strong>GET /task/{{task_id}}/progress</strong> - прогресс выполнения</li>
            </ul>
            
            <p><em>Внутренние эндпоинты доступны только из Docker-сети</em></p>
        </body>
    </html>
    '''


@dataclass
class WorkerTask:
    worker_url: str
    local_task_id: str
    idx_start: int
    idx_end: int
    word: str
    task_id: str
    status: str = "PENDING"
    last_processed_index: int = None
    found_words: List[str] = field(default_factory=list)
    last_update_time: float = field(default_factory=time.time)
    error: Optional[str] = None
    retry_count: int = 0

class TaskManager:
    def __init__(self):
        self.active_tasks: Dict[str, List[WorkerTask]] = {}
        self.loop = asyncio.new_event_loop()
        self.monitor_task = None
        self.dead_workers = set()
        self.dead_lock = threading.Lock()
        self._start_monitor()
    
    def _start_monitor(self):
        def run_monitor():
            asyncio.set_event_loop(self.loop)
            self.monitor_task = self.loop.create_task(self._monitor_workers())
            self.loop.run_forever()
        
        thread = threading.Thread(target=run_monitor, daemon=True)
        thread.start()
    
    async def _monitor_workers(self):
        while True:
            try:
                await asyncio.sleep(WAITING_TIME)
                
                for task_id, workers in list(self.active_tasks.items()):
                    if not workers:
                        continue
                    
                    for worker in workers:
                        now = time.time()

                        if worker.status == "IN_PROGRESS":
                            time_since_update = now - worker.last_update_time

                            if time_since_update > TIMEOUT_WORKER_TIME:
                                await self._redistribute_worker_task(worker, task_id)
                                worker.retry_count += 1

                        elif worker.status == "ERROR":
                            time_since_error = now - worker.last_update_time

                            if (
                                time_since_error > TIMEOUT_WORKER_TIME and
                                worker.retry_count < RETRIES_MAX_NUMBER
                            ):
                                logger.warning(f"Retrying failed worker {worker.local_task_id}")

                                await self._redistribute_worker_task(worker, task_id)
                                worker.retry_count = RETRIES_MAX_NUMBER

                    if all(w.status in ["COMPLETED", "ERROR"] for w in workers):
                        await self._finalize_task(task_id)
            except Exception as e:
                logger.exception(f"Monitor crashed iteration: {e}")
    
    async def _redistribute_worker_task(self, failed_worker: WorkerTask, task_id: str):
        with self.dead_lock:
            self.dead_workers.add(failed_worker.worker_url)

        available_workers = [
            w for w in WORKERS
            if w not in self.dead_workers and w != failed_worker.worker_url
        ]
        
        if not available_workers:
            logger.warning(f"No available workers to redistribute task {task_id}")
            failed_worker.status = "ERROR"
            return
        
        target_worker = random.choice(available_workers)
        
        last_processed = failed_worker.last_processed_index or failed_worker.idx_start
        remaining_range = [last_processed, failed_worker.idx_end]
        
        if remaining_range[0] >= remaining_range[1]:
            return
        
        logger.info(f"Redistributing range [{remaining_range[0]}, {remaining_range[1]}) "
                   f"from {failed_worker.local_task_id}")
        
        new_worker_task = WorkerTask(
            worker_url=target_worker,
            local_task_id=f"{failed_worker.local_task_id}_redistributed",
            idx_start=remaining_range[0],
            idx_end=remaining_range[1],
            word=failed_worker.word,
            task_id=task_id
        )
        
        self.active_tasks[task_id].append(new_worker_task)
        
        asyncio.create_task(self._execute_worker_task(new_worker_task))
        
        failed_worker.status = "ERROR"
    
    async def _execute_worker_task(self, worker_task: WorkerTask):
        worker_task.status = "IN_PROGRESS"
        worker_task.last_update_time = time.time()
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    worker_task.worker_url+"/internal/api/worker/hash/crack/task",
                    json={
                        "word": worker_task.word,
                        "idx_start": worker_task.idx_start,
                        "idx_end": worker_task.idx_end,
                        "task_id": worker_task.task_id,
                        "local_task_id": worker_task.local_task_id,
                        "last_processed_index": worker_task.last_processed_index,
                        "found_words": worker_task.found_words
                    },
                    timeout=aiohttp.ClientTimeout(total=300)
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        worker_task.status = "COMPLETED"
                        if result and result.get("received_word"):
                            current_words = set(worker_task.found_words)
                            current_words.update(result["received_word"])
                            worker_task.found_words = list(current_words)
                        
                        with status_lock:
                            if worker_task.task_id in task_statuses:
                                task_statuses[worker_task.task_id][worker_task.local_task_id] = {
                                    "status": "COMPLETED",
                                    "timestamp": time.time(),
                                    "found_words": worker_task.found_words,
                                    "range": f"[{worker_task.idx_start}, {worker_task.idx_end})",
                                    "progress": 100
                                }
                    else:
                        worker_task.status = "ERROR"
                        worker_task.error = f"HTTP {response.status}"
                        
        except Exception as e:
            worker_task.status = "ERROR"
            worker_task.error = str(e)

            with self.dead_lock:
                self.dead_workers.add(worker_task.worker_url)
        
        worker_task.last_update_time = time.time()
    
    async def _finalize_task(self, task_id: str):
        workers = self.active_tasks.get(task_id, [])
        
        logger.info(f"Finalizing request {task_id}. Local task statuses:")
        for w in workers:
            logger.info(f"  Local task {w.local_task_id}: status={w.status}, "
                    f"found={len(w.found_words)} words, error={w.error}")
        
        all_found_words = set()
        completed_ranges = []

        for worker in workers:
            if worker.found_words:
                all_found_words.update(worker.found_words)
            completed_ranges.append((worker.idx_start, worker.idx_end))
        
        completed_ranges.sort()
        merged = []

        for start, end in completed_ranges:
            if not merged:
                merged.append([start, end])
            else:
                last = merged[-1]
                if start <= last[1]: 
                    last[1] = max(last[1], end)
                else:
                    merged.append([start, end])
        
        total_covered = sum(end - start for start, end in merged)
        total_range = max(w.idx_end for w in workers) if workers else 0

        is_fully_covered = total_covered >= total_range

        final_status = "COMPLETED" if is_fully_covered else "ERROR"
        
        logger.info(f"Task {task_id} final status: {final_status}, "
                    f"total words found: {len(all_found_words)}")
        
        with status_lock:
            if task_id in task_statuses:
                task_statuses[task_id]["_final"] = {
                    "status": final_status,
                    "found_words": list(all_found_words),
                    "progress": 100 if is_fully_covered else 0
                }
        
        if task_id in self.active_tasks:
            del self.active_tasks[task_id]
            logger.info(f"Removed task {task_id} from active tasks")
    
    def create_task(self, word: str, length: int) -> str:
        task_id = str(uuid.uuid4())
        
        total_combinations = sum(len(ALPHABET) ** i for i in range(1, length + 1))
        chunk_size = total_combinations // len(WORKERS)
        
        workers = []
        
        with status_lock:
            task_statuses[task_id] = {}
        
        for i, worker_url in enumerate(WORKERS):
            idx_start = i * chunk_size
            if i == len(WORKERS) - 1:
                idx_end = total_combinations
            else:
                idx_end = (i + 1) * chunk_size
            
            worker_task = WorkerTask(
                worker_url=worker_url,
                local_task_id=f"{task_id}_{i + 1}",
                idx_start=idx_start,
                idx_end=idx_end,
                word=word,
                task_id=task_id
            )
            
            workers.append(worker_task)
            
            with status_lock:
                task_statuses[task_id][f"{task_id}_{i}"] = {
                    "status": "PENDING",
                    "range": f"[{idx_start}, {idx_end})",
                    "progress": 0
                }
        
        self.active_tasks[task_id] = workers
        
        async def run_all():
            tasks = [self._execute_worker_task(worker) for worker in workers]
            await asyncio.gather(*tasks, return_exceptions=True)
        
        asyncio.run_coroutine_threadsafe(run_all(), self.loop)
        
        return task_id
    
    def update_worker_status(self, task_id: str, local_task_id: str, 
                        last_index: int, found_words: List[str]):
        if task_id in self.active_tasks:
            for worker in self.active_tasks[task_id]:
                if worker.local_task_id == local_task_id:
                    worker.last_processed_index = last_index
                    if found_words:
                        existing = set(worker.found_words)
                        existing.update(found_words)
                        worker.found_words = list(existing)
                    worker.last_update_time = time.time()
                    break

        with status_lock:
            if task_id in task_statuses and local_task_id in task_statuses[task_id]:
                progress = 0
                if task_id in self.active_tasks:
                    for w in self.active_tasks[task_id]:
                        if w.local_task_id == local_task_id and w.idx_end > w.idx_start:
                            progress = ((last_index - w.idx_start) / 
                                    (w.idx_end - w.idx_start) * 100)
                
                current_words = task_statuses[task_id][local_task_id].get("found_words", [])
                all_words = list(set(current_words + found_words))
                
                task_statuses[task_id][local_task_id].update({
                    "status": "IN_PROGRESS",
                    "timestamp": time.time(),
                    "last_index": last_index,
                    "found_words": all_words,
                    "progress": round(progress, 2)
                })

task_manager = TaskManager()

@app.route("/api/hash/crack", methods=["POST"])
def task():
    data = request.get_json()

    if "word" not in data:
        return jsonify({"error": "word is required"}), 400
    if "length" not in data: 
        return jsonify({"error": "length is required"}), 400

    word = data["word"]
    length = int(data['length'])

    task_id = task_manager.create_task(word, length)
    
    return jsonify({"task_id": task_id})

@app.route("/status", methods=["POST"])
def receive_status():
    data = request.get_json()
    task_id = data.get("task_id")
    local_task_id = data.get("local_task_id")
    last_index = data.get("last_index")
    found_words = data.get("found_words", [])
    
    if found_words:
        logger.info(f"Status update - Task: {task_id}, Local task: {local_task_id}, "
                   f"Last index: {last_index}, Found words: {found_words}")
    else:
        logger.info(f"Status update - Task: {task_id}, Local task: {local_task_id}, "
                f"Last index: {last_index}, No words yet found")
    
    task_manager.update_worker_status(task_id, local_task_id, last_index, found_words)
    
    return jsonify({"status": "received"})

@app.route("/api/hash/status", methods=["GET"])
def get_task_status():
    task_id = request.args.get("requestId")
    
    if not task_id:
        return jsonify({"error": "requestId parameter is required"}), 400
    
    with status_lock:
        if task_id not in task_statuses:
            return jsonify({"error": "Task not found"}), 404
        
        task_data = task_statuses[task_id]
        
        total_progress = 0
        worker_count = 0
        all_found_words = set()
        
        for local_task_id, worker_data in task_data.items():
            if local_task_id.startswith("_"):
                continue
            if "progress" in worker_data:
                total_progress += worker_data["progress"]
                worker_count += 1
            if "found_words" in worker_data:
                all_found_words.update(worker_data["found_words"])
        
        if "_final" in task_data:
            final_status = task_data["_final"]["status"]
            if final_status == "COMPLETED":
                return jsonify({
                    "status": "COMPLETED",
                    "progress": 100,
                    "data": list(all_found_words)
                })
            else:
                return jsonify({
                    "status": "ERROR",
                    "progress": round(total_progress / worker_count, 2) if worker_count else 0,
                    "data": list(all_found_words)
                })
        
        avg_progress = total_progress / worker_count if worker_count else 0
        
        return jsonify({
            "status": "IN_PROGRESS",
            "progress": round(avg_progress, 2),
            "data": list(all_found_words)
        })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True, threaded=True)