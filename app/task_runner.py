from queue import Queue,Empty
from threading import Thread, Event,Lock
import time
import os
import json
import pandas as pd

class ThreadPool:
    def __init__(self):
        # You must implement a ThreadPool of TaskRunners
        # Your ThreadPool should check if an environment variable TP_NUM_OF_THREADS is defined
        # If the env var is defined, that is the number of threads to be used by the thread pool
        # Otherwise, you are to use what the hardware concurrency allows
        # You are free to write your implementation as you see fit, but
        # You must NOT:
        #   * create more threads than the hardware concurrency allows
        #   * recreate threads for each task
        self.task_queue = Queue()
        self.task_statuses={}
        self.lock = Lock()
        self.workers = [TaskRunner(self.task_queue,self.task_statuses,self.lock) for _ in range(int(os.getenv('TP_NUM_OF_THREADS', os.cpu_count())))]

    def start(self):
        for worker in self.workers:
            worker.start()

    def submit_task(self, task,task_id):
        task = Task(task,task_id)
        self.task_queue.put(task)
    
    def wait_completion(self):
        self.task_queue.join()
        for worker in self.workers:
            worker.join()

    def stop(self):
        self.wait_completion()
       
    def get_tasks_statuses(self):
        return self.task_statuses

    def get_task_status(self,task_id):
        if task_id.isdigit() and int(task_id) in self.task_statuses.keys():
            return self.task_statuses[int(task_id)]
        else:
            return {
                    "status": "error",
                    "reason": "Invalid job_id"
                    }

class TaskRunner(Thread):
    def __init__(self, task_queue,task_statuses,lock):
        super().__init__()
        self.task_queue = task_queue
        self.task_statuses = task_statuses
        self.lock = lock

    def run(self):
        while True:
            try:  
                # Get pending job
                job = self.task_queue.get(timeout=10)
                if job is None:
                    break  # Stop the thread if None task is received
                # Execute the task and save the result to disk
                task = job.task
                task_id = job.task_id  
             
                with self.lock:
                    self.task_statuses[task_id] = {"status": "running"}
                    
                result = self.execute_task(task, task_id)
                with self.lock:
                    self.task_statuses[task_id] = {"status": "done", "data": result}
            except Empty:
                continue
            except Exception as e:
                with self.lock:
                    self.task_statuses[task_id] = {"status": "error", "error": e}
          
            self.task_queue.task_done()
    
            # Execute the job and save the result to disk

    def execute_task(self, task,task_id):
      # Execute the task logic and save the result to disk
        if not os.path.exists("results"):
            os.makedirs("results")
        result = task()
        if isinstance(result, pd.Series):
            result = result.to_dict()
        file_path = os.path.join("results",f"out-{task_id}.json")
        with open(file_path,"w") as f:
            json.dump(result,f)
            # Repeat until graceful_shutdown      
        return result

class Task: 
    def __init__(self,task,task_id):
        self.task = task
        self.task_id = task_id
