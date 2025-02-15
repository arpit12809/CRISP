import flask 
from flask import Flask, render_template, jsonify, request
import requests
import sqlite3
import threading
from queue import Queue

app = Flask(__name__)
PORT = 8001

# Global variables for request interception
pending_requests = Queue()
intercept_enabled = True
request_lock = threading.Lock()

@app.route('/', methods=['GET'])
def home():
    database = sqlite3.connect('./Captured_requests.db')
    cursor = database.cursor()
    entries = cursor.execute('select * from all_requests order by Request_Number desc')
    
    # Get pending requests from queue
    with request_lock:
        pending = list(pending_requests.queue)
    
    return render_template("home.html", 
                         entries=entries,
                         pending_requests=pending,
                         intercept_enabled=intercept_enabled)

@app.route('/toggle_intercept', methods=['POST'])
def toggle_intercept():
    global intercept_enabled
    intercept_enabled = not intercept_enabled
    return jsonify({'intercept_enabled': intercept_enabled})

@app.route('/forward_request', methods=['POST'])
def forward_request():
    req_id = request.json.get('id')
    
    with request_lock:
        for _ in range(pending_requests.qsize()):
            item = pending_requests.get()
            if item['id'] == req_id:
                item['event'].set()  # Release the proxy thread
                return jsonify({'status': 'forwarded'})
            pending_requests.put(item)
    
    return jsonify({'status': 'not_found'}), 404

@app.route('/drop_request', methods=['POST'])
def drop_request():
    req_id = request.json.get('id')
    
    with request_lock:
        for _ in range(pending_requests.qsize()):
            item = pending_requests.get()
            if item['id'] == req_id:
                item['event'].set()  # Release the proxy thread
                pending_requests.task_done()
                return jsonify({'status': 'dropped'})
            pending_requests.put(item)
    
    return jsonify({'status': 'not_found'}), 404

def start_proxy_server():
    from proxy_server import start_proxy_server
    start_proxy_server()

if __name__ == "__main__":
    # Start proxy server in background thread
    proxy_thread = threading.Thread(target=start_proxy_server, daemon=True)
    proxy_thread.start()
    
    app.run(port=PORT)
