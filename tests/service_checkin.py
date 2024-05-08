import socket
import threading
import json

def send_message(message):
    server_ip = '10.175.19.128' ### outbound ip of renderserver
    server_port = 12121

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        client_socket.connect((server_ip, server_port))
        client_socket.send(message.encode('utf-8'))

        response = client_socket.recv(1024).decode('utf-8')
        print("Response:", response)

    finally:
        client_socket.close()

def thread_function(message):
    send_message(message)

if __name__=="__main__":
    threads = []
    num_calls = 1
    for i in range(num_calls):
        request = {"Type": "PreCalc", "Payload": "/mnt/x/PROJECTS/houdini/sequences/sh/sh_0070/comp/work/nuke/Comp/sh_0070_debug.v001.nk"}
        message = json.dumps(request)
        thread = threading.Thread(target=thread_function, args=[message])
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()