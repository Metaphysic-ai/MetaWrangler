import nuke
import sys
import os
import socket
import json

sys.path.insert(0, "/mnt/x/PROJECTS/software/nuke/python")
sys.path.insert(0, "/mnt/x/PROJECTS/software/shotgrid/tk-core/python")
sys.path.insert(0, "/mnt/x/PROJECTS/software/nuke")

if os.name == "nt":
    root =  "X:/PROJECTS"
    os.environ['PROJECT_ROOT'] = root
    dl_root =  "C:/Program Files/Thinkbox/Deadline10/bin"
    os.environ['DEADLINE_PATH'] = dl_root
if os.name == "posix":
    if not os.getenv("PROJECT_ROOT"):
        root =  "/mnt/data/DGX_SHARE/SHOTGRID_SYNC/PROJECTS"
        os.environ['PROJECT_ROOT'] = root
        dl_root =  "/opt/Thinkbox/Deadline10/bin"
        os.environ['DEADLINE_PATH'] = dl_root

# pipeline_root = '/'.join(nuke_env.split('/')[:-1]) + '/'
root = f"{os.getenv('PROJECT_ROOT')}"
os.environ['PIPELINE_ROOT'] = root

##Pluggin path##
sys.path.insert(0, '${PROJECT_ROOT}/software/nuke/gizmos' )
sys.path.insert(0, '${PROJECT_ROOT}/software/nuke/python' )
# nuke.pluginAddPath( './Python' )
sys.path.insert(0, '${PROJECT_ROOT}/software/nuke/plugins' )
sys.path.insert(0, '${PROJECT_ROOT}/software/nuke/plugins/animatedSnap3D' )
sys.path.insert(0, '${PROJECT_ROOT}/software/nuke/gizmos/backdrops' )
sys.path.insert(0, '${PROJECT_ROOT}/software/nuke/fonts' )
sys.path.insert(0, '${PROJECT_ROOT}/software/nuke/icons' )
sys.path.insert(0, '${PROJECT_ROOT}/software/nuke/callbacks' )


# nuke.pluginAddPath('X:/PROJECTS/nuke/')
# nuke.pluginAddPath(os.path.expandvars('${PROJECT_ROOT}/software/nuke/'))
nuke.pluginAddPath(os.path.expandvars('${PROJECT_ROOT}/software/nuke/plugins'))
nuke.pluginAddPath(os.path.expandvars('${PROJECT_ROOT}/software/nuke/plugins/NNSuperResolution'))
nuke.pluginAddPath(os.path.expandvars('${PROJECT_ROOT}/software/nuke/plugins/NNFlowVector'))
nuke.pluginAddPath(os.path.expandvars('${PROJECT_ROOT}/software/nuke/plugins/NukeSurvivalToolkit'))
nuke.pluginAddPath(os.path.expandvars('${PROJECT_ROOT}/software/nuke/plugins/pixelfudger3'))
nuke.pluginAddPath(os.path.expandvars('${PROJECT_ROOT}/software/nuke/plugins/RIFE'))
nuke.pluginAddPath(os.path.expandvars('${PROJECT_ROOT}/software/nuke/python/utils'))
nuke.pluginAddPath(os.path.expandvars('${PROJECT_ROOT}/software/nuke/python/utils/rvSync'))
nuke.pluginAddPath(os.path.expandvars('${PROJECT_ROOT}/software/nuke/python/utils/animation/'))
nuke.pluginAddPath(os.path.expandvars('${PROJECT_ROOT}/software/nuke/plugins/RawPredTransformOp/')) #TEMP FOR TESTING
if os.name == "nt":
    nuke.pluginAddPath(os.path.expandvars('${PROJECT_ROOT}/software/nuke/plugins/3DE4/windows/'))
if os.name == "posix":
    nuke.pluginAddPath(os.path.expandvars('${PROJECT_ROOT}/software/nuke/plugins/3DE4/linux/'))

import sgtk
import Deadline_sgConvertToWrite
Deadline_sgConvertToWrite.Deadline_sgConvertToWrite()
### Attempt to connect to MetaWrangler. Skip if connection fails.

server_ip = '10.175.19.128'  # outbound IP of renderserver
server_port = 12121

client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

payload_dict = {nuke.root().name(): {}}
write_nodes = []
for node in nuke.allNodes("Write", recurseGroups=True):
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

            if node_class not in ['Write'] or current_node == node:
                for dep in current_node.dependencies():
                    if dep not in seen:
                        stack.append(dep)
            else:
                if current_node in write_nodes and current_node != node:
                    nodes.add((node_class, node_name))
    return nodes

for node in write_nodes:
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
