import nuke
import sys
import os
import socket
import json

### Attempt to connect to MetaWrangler. Skip if connection fails.

server_ip = '10.175.19.128'  # outbound IP of renderserver
server_port = 12121

client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

payload_dict = {nuke.root().name(): {}}
write_nodes = []
for node in nuke.allNodes("Write", recurseGroups=True):
    write_nodes.append(node)
for node in nuke.allNodes("WriteTank", recurseGroups=True):
    write_nodes.append(node)
print("WRITE_NODES", write_nodes)

def get_dependencies(node, write_nodes):
    seen = set()
    stack = [node]
    nodes = set()

    while stack:
        current_node = stack.pop()
        if current_node not in seen:
            seen.add(current_node)
            node_class = current_node.Class()
            node_name = current_node.name()

            if (node_class, node_name) != (node.Class(), node.name()):
                if node_class not in ['Dot', 'BackdropNode']:
                    nodes.add((node_class, node_name))

            if node_class not in ['Write', 'WriteTank'] or current_node == node:
                for dep in current_node.dependencies():
                    if dep not in seen:
                        stack.append(dep)
            else:
                if current_node in write_nodes and current_node != node:
                    nodes.add((node_class, node_name))
    return nodes

for node in write_nodes:
    print(get_dependencies(node, write_nodes))
    payload_dict[nuke.root().name()][node.name()] = get_dependencies(node, write_nodes)

try:
    request = {"Type": "PreCalc", "Payload": payload_dict}
    message = json.dumps(request)
    client_socket.connect((server_ip, server_port))
    client_socket.sendall(message.encode('utf-8'))
    print("Sending script sample to MetaWrangler to prepare for submission.")

    response = client_socket.recv(1024).decode('utf-8')
    print("Response:", response)

except socket.error as e:
    print(f"Socket error occurred: {e}")

except Exception as e:
    print(f"An unexpected error occurred: {e}")

finally:
    client_socket.close()
