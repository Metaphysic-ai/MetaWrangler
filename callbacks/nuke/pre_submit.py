def check_cdl_entries():
    raise NotImplementedError
    ### TODO: Check if CDL nodes are looking for the correct shot_code

def check_env():
    ### TODO: Check if the environment sent matches the submission info
    raise NotImplementedError
    import os
    os.environ["OPENCV_IO_ENABLE_OPENEXR"] = "1"

def mute_viewer():
    ### TODO: Disable or delete viewer node vs "Bad Viewer" error.
    raise NotImplementedError

def replace_outdated_rawpred():
    ### TODO: If we find an old version of rawpred, replace it with a new one.
    raise NotImplementedError

def send_dependency_graph():
    import nuke
    payload_dict = {nuke.root().name(): {}}
    write_nodes = []
    for node in nuke.allNodes("Write", recurseGroups=True):
        write_nodes.append(node)
    for node in nuke.allNodes("WriteTank", recurseGroups=True):
        write_nodes.append(node)

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
        payload_dict[nuke.root().name()][node.name()] = get_dependencies(node, write_nodes)

    print(payload_dict)

    # import socket
    # import json
    # ### Attempt to connect to MetaWrangler. Skip if connection fails.
    #
    # server_ip = '10.175.19.128'  # outbound IP of renderserver
    # server_port = 12121
    #
    # client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #
    # try:
    #     request = {"Type": "DependencyGraph", "Payload": payload_dict}
    #     message = json.dumps(request)
    #     client_socket.connect((server_ip, server_port))
    #     client_socket.sendall(message.encode('utf-8'))
    #     print("Sending script sample to MetaWrangler to prepare for submission.")
    #
    #     response = client_socket.recv(1024).decode('utf-8')
    #     print("Response:", response)
    #     return "Success"
    #
    # except socket.error as e:
    #     print(f"Socket error occurred: {e}")
    #
    # except Exception as e:
    #     print(f"An unexpected error occurred: {e}")
    #
    # finally:
    #     client_socket.close()