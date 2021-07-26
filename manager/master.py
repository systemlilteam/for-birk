from flask import Flask, request, Response
from flask_apscheduler import APScheduler
from queue import Queue
from datetime import datetime
import config
import numpy as np
import requests
import json
import time
import pickle
import logging

def dump_Data(data):
    pickle_out = open('health.pkl',"wb")
    pickle.dump(data, pickle_out)
    pickle_out.close()

def import_data(f):
    pickle_in = open(f,"rb")
    return pickle.load(pickle_in)

health_status = {}
dump_Data(health_status)
all_range = range(0,100000001)
queue = Queue()

logging.basicConfig(
    level=logging.INFO,
    handlers=[ logging.FileHandler(r"DEBUG_Master.log"), logging.StreamHandler() ],
    format='%(asctime)s:%(levelname)s:%(message)s', 
)


def get_range_for_worker(ranges,workers_number):
    ranges = list(np.array_split(ranges,workers_number))
    return ranges

def init_agents():
    for worker in range(1,config.WORKERS_NUMBER+1):
        port = worker + config.BASE_PORT
        health_status = import_data(config.PICKLE)
        health_status[str(port)] = {}
        dump_Data(health_status)
        res = requests.get(f'http://localhost:{port}/api/init')
        logging.info(f'agent init {res.text}')

def send_ranges_to_workers(hash):
    ranges_for_workers = get_range_for_worker(all_range,config.WORKERS_NUMBER)
    port = 0
    for worker in range(1,config.WORKERS_NUMBER+1):
        port = worker + config.BASE_PORT
        data_for_worker ={'data':list([str(ranges_for_workers[worker-1][0]),str(ranges_for_workers[worker-1][-1])]),'hash':hash}
        health_status = import_data(config.PICKLE)
        health_status[str(port)] = {'hash':hash,'heartbeat':0,'status':'start'}
        dump_Data(health_status)
        logging.info(f"port is {port}")
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        requests.post(f'http://localhost:{port}/api/add/range',data=json.dumps(data_for_worker),headers=headers)
        logging.info(f"sent data to worker {port}")

def write_solution(health_status):
    # try:
    logging.info(f'write solution to {config.SOLUTION_FILE} file')      
    for worker in range(1,config.WORKERS_NUMBER+1):
        port = worker + config.BASE_PORT
        try:
            hash = health_status[str(port)]['hash']
            solution = health_status[str(port)]['solution']
            with open(config.SOLUTION_FILE,'w') as f:
                f.write(f'HASH: {hash} SOLUTION: {solution}')
        except:
            logging.info('Error Writing to file.')
# except:
    #     logging.error(f"can\'t write to {config.SOLUTION_FILE} file")

def init_hash_list():
    hashes = []
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

# def handle_hashes_list(hashes):
#     for hash in hashes:
#         send_ranges_to_workers()
#         while True:
            

# hashes = init_hash_list()
init_agents()

app = Flask(__name__)
scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()

INTERVAL_TASK_ID = 'CheckHealth'

# def handle_hashes():
#     hashes = init_hash_list()
#     logging.info(f'Hashes List {hashes}')
#     for hash in hashes:
#         health_status = {}
#         dump_Data(health_status)
#         send_ranges_to_workers(hash)
#         for worker in range(1,config.WORKERS_NUMBER+1):
#             try:
#                 health_status = import_data(health_status)
#                 port = worker + config.BASE_PORT
#                 # try:
#                 tdelta = datetime.now() - health_status[str(port)]['heartbeat']
#                 if  tdelta.total_seconds() > 10:
#                     logging.info(f"Error With Worker {port}")
#                 if health_status[str(port)]['checkStatus'] != "done" and health_status[str(port)]['checkStatus'] != "start" :
#                     logging.info(f'Worker Found Solution, {health_status[str(port)]}')
#                     write_solution(health_status)
#                 elif health_status[str(worker)]['checkStatus'] == "start" and health_status[str(worker)]['status'] != 'alive':
#                     logging.info(f'Waiting for Worker {port} ...')  
#             except Exception as e:
#                 logging.info(f'Error {e}')
#             # except:
#             #     logging.error('Error')

def check_health(): 
    for worker in range(1,config.WORKERS_NUMBER+1):
        try:
            health_status = import_data(health_status)
            port = worker + config.BASE_PORT
            # try:
            tdelta = datetime.now() - health_status[str(port)]['heartbeat']
            if  tdelta.total_seconds() > 10:
                logging.info(f"Error With Worker {port}")
            if health_status[str(port)]['checkStatus'] != "done" and health_status[str(port)]['checkStatus'] != "start" :
                logging.info(f'Worker Found Solution, {health_status[str(port)]}')
                write_solution(health_status)
            elif health_status[str(worker)]['checkStatus'] == "start" and health_status[str(worker)]['status'] != 'alive':
                logging.info(f'Waiting for Worker {port} ...')  
        except Exception as e:
            logging.info(f'Error {e}')

scheduler.add_job(id=INTERVAL_TASK_ID, func=check_health, trigger='interval', seconds=12)



@app.route("/api/report/solution",methods=['POST'])
def get_solution():
    json_req = request.json
    logging.info(json_req)
    health_status = import_data(config.PICKLE)
    health_status[json_req['id']].update({'checkStatus':json_req['checkStatus'],'solution': json_req['solution'],'hash':json_req['hash']})
    logging.info(health_status)
    dump_Data(health_status)
    status_code = Response(status=200)
    logging.info(f"got solution from worker {json_req['id']}")
    return status_code

@app.route("/api/report/health",methods=['POST'])
def get_health():
    json_req = request.json
    logging.info(json_req)
    health_status = import_data(config.PICKLE)
    if 'status' not in health_status[json_req['id']] and 'heartbeat' not in health_status[json_req['id']]:
        health_status[json_req['id']].update({'status':json_req['status'],'heartbeat':time.time()})
    else:
        health_status[json_req['id']]['status'] = json_req['status']
        health_status[json_req['id']]['heartbeat'] = time.time()
    logging.info(health_status)
    dump_Data(health_status)
    status_code = Response(status=200)
    logging.info(f"get heartbeat from worker {json_req['id']}")
    return status_code



if __name__ == "__main__":
    app.run(host="0.0.0.0",port=5000,debug=False)