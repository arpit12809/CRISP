import socket
import threading
import sqlite3
import signal
import sys
import time
import json
from websockets.server import serve
import asyncio
import queue

intercept_queue = queue.Queue()
is_intercepting = False
connected_clients = set()

async def websocket_handler(websocket):
    """Handle WebSocket connections from the UI"""
    global connected_clients
    connected_clients.add(websocket)
    try:
        async for message in websocket:
            data = json.loads(message)
            command = data.get('command')
            
            if command == 'toggle_intercept':
                global is_intercepting
                is_intercepting = data.get('enabled', False)
            elif command == 'forward':
                if not intercept_queue.empty():
                    intercept_data = intercept_queue.get()
                    intercept_data['action'] = 'forward'
                    await process_intercepted_request(intercept_data)
            elif command == 'drop':
                if not intercept_queue.empty():
                    intercept_queue.get()
    finally:
        connected_clients.remove(websocket)

async def notify_ui(request_data):
    """Send intercepted request to UI"""
    if connected_clients:
        message = json.dumps({
            'type': 'intercepted_request',
            'data': request_data
        })
        await asyncio.gather(
            *[client.send(message) for client in connected_clients]
        )

async def process_intercepted_request(intercept_data):
    """Process an intercepted request based on UI action"""
    if intercept_data.get('action') == 'forward':
        client_socket = intercept_data['client_socket']
        request = intercept_data['request']
        try:
            host, port = extract_host_port_from_request(request)
            destination_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            destination_socket.connect((host, port))
            destination_socket.sendall(request)
            response = handle_response(destination_socket, client_socket)
            store_request_response(request, response)
            
            destination_socket.close()
        except Exception as e:
            print(f"Error processing request: {e}")
        finally:
            client_socket.close()

def setup_database():
    database = sqlite3.connect('Captured_requests.db')
    cursor = database.cursor()
    try:
        cursor.execute('drop table all_requests')
    except:
        pass
    cursor.execute('create table all_requests (Request_Number float, Request text, Response text)')
    database.close()

def store_request_response(request, response):
    database = sqlite3.connect('Captured_requests.db')
    cursor = database.cursor()
    cursor.execute('insert into all_requests values (?,?,?)', 
                  (time.time(), request.decode(), response.decode()))
    database.commit()
    database.close()

def handle_response(destination_socket, client_socket):
    response = bytes()
    destination_socket.settimeout(10.0)
    while True:
        try:
            data = destination_socket.recv(2*1024)
            response += data
            if len(data) > 0:
                client_socket.sendall(data)
            else:
                break
        except (socket.timeout, TimeoutError):
            break
    return response

def handle_client_request(client_socket):
    print("Received request:")
    request = b''
    client_socket.setblocking(False)
    
    while True:
        try:
            data = client_socket.recv(2*1024)
            request = request + data
            print(f"{data.decode('utf-8')}")
        except:
            break
    
    if is_intercepting:
        intercept_data = {
            'client_socket': client_socket,
            'request': request,
            'timestamp': time.time()
        }
        intercept_queue.put(intercept_data)
        asyncio.run(notify_ui({
            'request': request.decode(),
            'timestamp': time.time()
        }))
    else:
        asyncio.run(process_intercepted_request({
            'action': 'forward',
            'client_socket': client_socket,
            'request': request
        }))

def extract_host_port_from_request(request):
    host_string_start = request.find(b'Host: ') + len(b'Host: ')
    host_string_end = request.find(b'\r\n', host_string_start)
    host_string = request[host_string_start:host_string_end].decode('utf-8')
    webserver_pos = host_string.find("/")
    if webserver_pos == -1:
        webserver_pos = len(host_string)
    port_pos = host_string.find(":")
    if port_pos == -1 or webserver_pos < port_pos:
        port = 80
        host = host_string[:webserver_pos]
    else:
        port = int((host_string[(port_pos + 1):])[:webserver_pos - port_pos - 1])
        host = host_string[:port_pos]
    return host, port

async def start_websocket_server():
    async with serve(websocket_handler, "localhost", 8889):
        await asyncio.Future()

def start_proxy_server():
    setup_database()
    port = 8888
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('127.0.0.1', port))
    server.listen(20)
    print(f"Proxy server listening on port {port}...")
    
    while True:
        try:
            client_socket, addr = server.accept()
            print(f"Accepted connection from {addr[0]}:{addr[1]}")
            client_handler = threading.Thread(
                target=handle_client_request,
                args=(client_socket,)
            )
            client_handler.start()
        except KeyboardInterrupt:
            print("Shutting down server...")
            server.close()
            sys.exit(0)

if __name__ == "__main__":
    websocket_thread = threading.Thread(
        target=lambda: asyncio.run(start_websocket_server())
    )
    websocket_thread.start()
    start_proxy_server()