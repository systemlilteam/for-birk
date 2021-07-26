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

def compare_hash(range_for_compare,hash,solution,q):
    logging.info(f' Range for compare {int(range_for_compare[0])}')
    range_start = int(range_for_compare[0])
    range_end = int(range_for_compare[-1])+1
    for num in range(range_start,range_end):
        # if status isn't solution
        str_num = str(num)
        num_for_check = f'05{str_num[0]}-{str_num[1:]}'
        calced_hash = hashlib.md5(num_for_check.encode())
        if str(calced_hash.hexdigest()) == hash:
            solution = num_for_check
            q.put(solution)
            logging.info(f'found solution, {solution}')
            break
        # else:
        #     logging.info(f'no solution found yet for {num_for_check}, hash {calced_hash.hexdigest()}')
        elif(str(calced_hash.hexdigest()) == '7c81f40afa24eef05490984b6898dab'):
            logging.info(f'no solution found yet for {num_for_check}, hash {calced_hash.hexdigest()}')
            
        
def heartbeat(ID,hash,q):
    logging.info('Entered heartbeat')
    # if not q.empty():
    logging.info(ID)
    if ID != "":
        logging.info(f"id is {ID}")
        solution = q.get(False)
        logging.info(f'My ID: {ID}, Solution {solution}, hash {hash}')
        heartbeat_data =  {'id':str(ID),'status':solution,'hash':hash}
        try:
            headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
            requests.post(f'{config.MASTER_URL}/api/report/health',data = json.dumps(heartbeat_data),headers=headers)
            logging.info('Sent heatbeat to master')
        except:
            logging.info('Can not reach Master')


app = Flask(__name__)
scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()

INTERVAL_TASK_ID = 'heartbeat'

scheduler.add_job(id=INTERVAL_TASK_ID, func=heartbeat, trigger='interval', seconds=10,args=(ID,hash,queue))

@app.route("/api/add/range",methods=['POST'])
def get_range():
    json_req = request.json
    logging.info(json_req)
    solution = "no"
    ID = json_req['id']
    range_for_compare = json_req['data']
    hash = json_req['hash']
    logging.info(f'range {range_for_compare}')
    # threading.Thread(target=heartbeat,args=(ID,hash,queue,)).start()
    threading.Thread(target=compare_hash,args=(range_for_compare,hash,solution,queue,)).start()
    status_code = Response(status=200)
    return status_code

# @app.route("/api/wakeup",methods=['GET'])
# def get_wakeup():




if __name__ == "__main__":
    # threading.Thread(target=app.run(host="0.0.0.0",port=5001,debug=False, use_reloader=False,threaded=True)).start()
    app.run(host="0.0.0.0",port=5001,debug=True,use_reloader=False)
    # threading.Thread(target=compare_hash).start()