import json
from flask import Flask, config, request, jsonify, Response                                                   
from time import sleep
from queue import Queue
from flask_apscheduler import APScheduler
import threading
import requests
import config
import json
import hashlib
import logging
import sys

ID = ""
range_for_compare = []
hash = ""
solution = "no"
queue = Queue()

logging.basicConfig(
    level=logging.INFO,
    handlers=[ logging.FileHandler(r"./DEBUG_Worker.log"), logging.StreamHandler() ],
    format='%(asctime)s:%(levelname)s:%(message)s', 
)

def compare_hash(range_for_compare,hash,solution):
    port = sys.argv[1]
    range_start = int(range_for_compare[0])
    range_end = int(range_for_compare[-1])+1
    for num in range(range_start,range_end):
        str_num = str(num)
        num_for_check = f'05{str_num[0]}-{str_num[1:]}'
        calced_hash = hashlib.md5(num_for_check.encode())
        if str(calced_hash.hexdigest()) == str(hash):
            solution = num_for_check
            solution_data =  {'id': str(sys.argv[1]),'solution': solution, 'checkStatus' : 'found', 'hash': str(hash) }
            headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
            requests.post(f'{config.MASTER_URL}/api/report/solution',data = json.dumps(solution_data),headers=headers)
            logging.info(f'found solution, {solution}')
            solution_data = {}
            break
        # elif(num_for_check == '051-23'):
        #     logging.info(f'no solution found yet for {num_for_check}, hash {calced_hash.hexdigest()} orig hash {hash}')
        else:
            logging.info(f'No solution Find yet for worker {port}')
            
        
def heartbeat():
    logging.info('Entered heartbeat')
    while True:
        heartbeat_data =  {'status':'alive', 'id':str(sys.argv[1])}
        try:
            headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
            requests.post(f'{config.MASTER_URL}/api/report/health',data = json.dumps(heartbeat_data),headers=headers)
            logging.info('Sent heatbeat to master')
        except:
            logging.info('Can not reach Master')
        sleep(10)

port = sys.argv[1]
app = Flask(__name__)

@app.route("/api/add/range",methods=['POST'])
def get_range():
    json_req = request.json
    logging.info(json_req)
    solution = "no"
    # ID = json_req['id']
    range_for_compare = json_req['data']
    hash = json_req['hash']
    logging.info(f'range {range_for_compare}')
    threading.Thread(target=compare_hash,args=(range_for_compare,hash,solution,)).start()
    # threading.Thread(target=heartbeat,args=(ID,hash,queue,)).start()
    status_code = Response(status=200)
    return status_code

@app.route("/api/init",methods=['GET'])
def init_worker():
    threading.Thread(target=heartbeat).start()
    return jsonify('Agent Initialized Successfuly!')



if __name__ == "__main__":
    # threading.Thread(target=app.run(host="0.0.0.0",port=5001,debug=False, use_reloader=False,threaded=True)).start()
    app.run(host="0.0.0.0",port=sys.argv[1],debug=True)
    # threading.Thread(target=compare_hash).start()