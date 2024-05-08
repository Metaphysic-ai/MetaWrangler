import socket

def send_message(message):
    server_ip = '10.175.19.128' ### outbound ip of renderserver
    server_port = 12345

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        client_socket.connect((server_ip, server_port))
        client_socket.send(message.encode('utf-8'))

        response = client_socket.recv(1024).decode('utf-8')
        print("Response:", response)

    finally:
        client_socket.close()