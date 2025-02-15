import socket
import threading
import sqlite3
import signal
import sys
import time

INTERCEPT_ENABLED = True

def setup_database():
    database = sqlite3.connect('Captured_requests.db')
    cursor = database.cursor()
    try:
        cursor.execute('drop table all_requests')
    except:
        pass
    cursor.execute('create table all_requests (Request_Number float, Request text, Response text)')
    database.close()

def intercept_request(request):
    """Intercepts the request and waits for user decision"""
    print("\n=== Intercepted HTTP Request ===")
    print(request.decode('utf-8', errors='replace'))
    
    while True:
        command = input("Enter 'forward' to send request or 'drop' to discard: ").strip().lower()
        if command == 'forward':
            return True
        elif command == 'drop':
            return False
        else:
            print("Invalid command. Please enter 'forward' or 'drop'.")

def handle_client_request(client_socket):
    database = sqlite3.connect('Captured_requests.db')
    cursor = database.cursor()

    request = b''
    client_socket.setblocking(False)
    while True:
        try:
            data = client_socket.recv(2*1024)
            request += data
            print(f"{data.decode('utf-8', errors='replace')}")
        except:
            break

    if INTERCEPT_ENABLED:
        print("\n[!] Request interception required!")
        if not intercept_request(request):
            print("[!] Request dropped by user")
            client_socket.close()
            cursor.close()
            database.close()
            return

    # Rest of the original forwarding logic
    host, port = extract_host_port_from_request(request)
    destination_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    try:
        destination_socket.connect((host, port))
        destination_socket.sendall(request)
        
        print("Received response:\n")
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
            except (TimeoutError, KeyboardInterrupt):
                break

        print(response.decode('utf-8', errors='replace'))
        cursor.execute('insert into all_requests values (?,?,?)', 
                      (time.time(), request.decode('utf-8', errors='replace'), 
                       response.decode('utf-8', errors='replace')))
                       
    finally:
        destination_socket.close()
        client_socket.close()
        print('<--------------------------------------------------------------------------------------->')
        print('The database is:')
        for row in cursor.execute('select * from all_requests order by Request_Number desc'):
            print(row)
        print('<--------------------------------------------------------------------------------------->')
        cursor.close()
        database.commit()
        database.close()


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

def start_proxy_server():
    signal.signal(signal.SIGINT, shutdown_server)
    global server, request_index
    setup_database()
    port = 8888
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('127.0.0.1', port))
    server.listen(20)
    print(f"Proxy server listening on port {port}...")
    while True:
        client_socket, addr = server.accept()
        print(f"Accepted connection from {addr[0]}:{addr[1]}")
        client_handler = threading.Thread(target=handle_client_request, args=(client_socket,))
        client_handler.start()

def shutdown_server(signal, frame):
    print('Shutting down server...')
    server.close()
    sys.exit(0)

if __name__ == "__main__":
    start_proxy_server()