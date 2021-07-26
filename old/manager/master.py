from flask import Flask, request, Response
from flask_apscheduler import APScheduler
import config
import numpy as np
import requests
import json
import time
import logging
import queue
import threading


q = queue.Queue()
health_status = {}
all_range = range(0,100000000)
solution = {}
hashes = []

logging.basicConfig(
    level=logging.INFO,
    handlers=[ logging.FileHandler(r"DEBUG_Master.log"), logging.StreamHandler() ],
    format='%(asctime)s:%(levelname)s:%(message)s'
)

def send_solution():
    try:
        logging.info(f'write solution to {config.SOLUTION_FILE} file')      
        with open(config.SOLUTION_FILE, 'a') as file:
            for hash, password in solution.items():
                file.write(f"Hash: {hash}, Password: {password} \n")
    except:
        logging.error(f"can\'t write to {config.SOLUTION_FILE} file")


def init_hash_list():
    try:
        logging.info(f'read hashes from {config.HASHES_FILE} file')      
        with open(config.HASHES_FILE, 'r') as f:
            lines = f.read()
            lines = lines.split('\n')
            for line in lines:
                hashes.append(line)
    except:
        logging.error(f"can\'t read {config.HASHES_FILE} file")
        exit(1)
    return hashes


def get_range_for_worker(ranges,workers_number):
    ranges = list(np.array_split(ranges,workers_number))
    return ranges


def send_ranges_to_workers():
    hash = hashes[0]
    ranges_for_workers = get_range_for_worker(all_range,config.WORKERS_NUMBER)
    port = 0
    for worker in range(1,config.WORKERS_NUMBER+1):
        port = worker + config.BASE_PORT
        data_for_worker = {'id':worker,'data':list([str(ranges_for_workers[worker-1][0]),str(ranges_for_workers[worker-1][-1])]),'hash':hash}
        health_status[str(worker)] = {'hash':hash,'heartbeat':0,'data_for_worker':data_for_worker}
        logging.info(f"port is {port}, worker {health_status[str(worker)]}")
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        requests.post(f'http://localhost:{port}/api/add/range',data=json.dumps(data_for_worker),headers=headers)
        logging.info("sent data to worker")


hashes = init_hash_list()
send_ranges_to_workers()
# threading.Thread(target=send_ranges_to_workers).start()
# threading.Thread(target=send_solution).start()


app = Flask(__name__)
scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()

INTERVAL_TASK_ID = 'CheckHealth'

def check_health():
    for worker in range(1,config.WORKERS_NUMBER+1):
        # try:
        if len(health_status) == config.WORKERS_NUMBER:
            if health_status[str(worker)]['status'] != "no" and health_status[str(worker)]['status'] != "done":
                logging.info(f'Worker Found Solution, {health_status[str(worker)]}')
                # q.task_done()
            else:
                logging.info('no solution yet')
        else:
            logging.info('Waiting for Workers...')    
        # except:
        #     logging.error('Error')


# def check_health():
    for worker in range(1,config.WORKERS_NUMBER+1):
        try:
            #status: start, no, solution, done
            health_worker = health_status[str(worker)]
            status = health_worker['status']
            logging.info(f'Worker status {status}')
            if status != "no" and status != "done" and status != "start":
                solution[status] = health_status[health_worker['hash']]
                logging.info(f'solution, {solution}')
                status = 'done'
                q.task_done()
                logging.info(f'Worker Found Solution, {health_worker}')
                #TODO: stop other workers work
                #TODO: kill worker thread of heartbeat
            else:
                if status != "done":
                    #TODO: status start or no
                    pass
        except Exception as e:
            logging.error(f'Error: {str(e)}')

# check_health()    

scheduler.add_job(id=INTERVAL_TASK_ID, func=check_health, trigger='interval', seconds=10)

#health_status['id'] = {'status':'no','hash':hash,'heartbeat':0, 'data_for_worker':data_for_worker}

@app.route("/api/report/health",methods=['POST'])
def get_health():
    json_req = request.json
    logging.info(json_req)
    health_status[json_req['id']]['status'] = json_req['status']
    health_status[json_req['id']]['hash'] = json_req['hash'] 
    health_status[json_req['id']]['heartbeat'] = time.time()
    logging.info(health_status)
    status_code = Response(status=200)
    logging.info(f"got heartbeat from worker {json_req['id']}")
    return status_code



if __name__ == "__main__":
    app.run(host="0.0.0.0",port=5000,debug=False)