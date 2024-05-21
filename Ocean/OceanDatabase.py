from managers.NukeManager import NukeManager
import re
import socket
import json
from datetime import datetime
import numpy as np
import time
import subprocess

class OceanDatabase:
    ### Fancy name for the database.
    def __init__(self, wrangler, vector_utils):
        self.SUPPORTED_REQUEST_TYPES = [
            "PreCalc",
            "GetProfile"
        ]
        self.wrangler = wrangler
        self.vector_utils = vector_utils

    def add_to_database(self, script_dependency_dict):
        nuke_script = self.vector_utils.parse_dependency_dict(script_dependency_dict)
        self.vector_utils.vectorize(nuke_script)
        nodes = set()
        for k, v in nuke_script.write_node_embeddings.items():
            for node_name, embedding in nuke_script.write_node_embeddings[k].items():
                nodes.add(node_name)
        for check_node in nodes:
            for k, v in nuke_script.write_node_embeddings.items():
                for node_name, embedding in nuke_script.write_node_embeddings[k].items():
                    if node_name == check_node:
                        if "compare" not in locals():
                            compare = embedding
                        elif not np.allclose(compare, embedding, atol=1e-05):
                            print(k, check_node, np.allclose(compare, embedding, atol=1e-05), "Compare embedding:", compare[:5], "Node:", embedding[:5])
            del compare


    def get_profile_args(self, script, write_nodes):
        args = {
                "id": 0,
                "info": {"estimated_success_rate": 1.0},
                "mem": 4,
                "cpus": 2,
                "gpu": False,
                "batch_size": 10,
                "min_time": 0,
                "max_time": 10,
                "pcomp_flag": False,
                "creation_time": str(datetime.now().strftime('%y%m%d_%H%M%S'))
                }
        return args

    def get_local_ip(self):
        try:
            # Create a dummy socket and connect to a well-known address (Google DNS)
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(("8.8.8.8", 80))
                ip = s.getsockname()[0]
        except Exception:
            ip = "Could not determine local IP"
        return ip

    def handle_client(self, client_socket):
        request = client_socket.recv(1024).decode('utf-8').strip()
        print(f"Received: {request}")
        response = f"{request['Type']} is not implemented yet."
        request = json.loads(request)
        if not request['Type'] in self.SUPPORTED_REQUEST_TYPES:
            response = f"A request of the type {request['Type']} is not supported."

        if request.get("Type") == "PreCalc":
            ### When a user opens a nuke script, we precalculate the profile to be ready at submission time.
            nuke_mng = NukeManager(request["Payload"])
            write_node_dependencies = nuke_mng.get_write_dependencies()
            self.add_to_database(write_node_dependencies)

        if request.get("Type") == "GetProfile":
            script_path = request["Payload"]["script_path"]
            write_node = request["Payload"]["write_node"]
            args = self.get_profile_args(script_path, write_node)
            response = json.dumps(args)

        client_socket.send(response.encode('utf-8'))
        client_socket.close()

    def run(self):
        import socket
        import subprocess
        import time
        from datetime import datetime
        print("Starting Ocean Database...")

        hostname = socket.gethostname()
        host = '0.0.0.0'
        port = 12123

        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((host, port))
        server_socket.listen(20)
        server_socket.setblocking(False)

        print(f"Ocean Database is listening on {self.get_local_ip()}:{port}")

        tick_times = []
        while True:
            loop_start = time.time() ### DEBUG: Analyse main loop performance, should probably stay <1-2s

            try:
                client_socket, client_address = server_socket.accept()
                print(f"Connection from {client_address} has been established!")
                self.handle_client(client_socket)
            except BlockingIOError:
                pass

            tick_times.append(time.time() - loop_start)  ### Do an average over how long we take per loop
            if len(tick_times) > 100:
                # print("### DEBUG: Estimated time spent per loop:", sum(tick_times) / len(tick_times), "Seconds.")
                tick_times = []

class Node: # TODO: deprecated.
    def __init__(self, name="", type="", knobs=[], num_inputs=None):
        self.name = name
        self.type = type
        self.knobs = knobs
        self.body = ""
        self.inputs = num_inputs
        self.root_node = None
        self.out_nodes = []
        self.in_nodes = []

    def __repr__(self):
        return f'Node(\'{self.type}\', {self.name})'

class Graph:
    def __init__(self, script, ignore_backdrops=True, ignore_dots=True):
        self.is_constructing = True
        self.graph_dict = {}
        self.script = script
        self.stack = []
        self.simplifiedDAG = {}
        self.nodes = []
        self.fill_graph_from_script_simplified(ignore_backdrops, ignore_dots)
        self.fill_DAG()

    def get_profile_for_script(self, script, write_node):
        ### TODO: smartify the thing
        return None

    def fill_DAG(self):
        for node in self.nodes:
            if node.type not in self.simplifiedDAG.keys():
                self.simplifiedDAG[node.type] = 1
            else:
                self.simplifiedDAG[node.type] += 1

    def fill_graph_from_script_simplified(self, ignore_backdrops=True, ignore_dots=True):
        start_parse = False
        bracket_stack = 0
        why = False
        # "Root {"
        with open(self.script, "r") as f:
            node = Node()
            for n, line in enumerate(f.readlines()):
                if "Root {" in line:
                    start_parse = True
                elif not start_parse:
                    continue
                if start_parse:
                    if " {" in line and bracket_stack == 0:
                        node.type = line.split(" ")[0]
                    if line.strip().startswith("inputs"):
                        node.inputs = line.split(" ")[-1]
                    if line.strip().startswith("name"):
                        node.name = line.split(" ")[-1].strip()
                    node.body += line.strip()+"\n"
                    bracket_stack += line.count("{")
                    bracket_stack -= line.count("}")
                    if bracket_stack == 0:
                        if not (ignore_backdrops and "backdrop" in node.type.lower()):
                            if not (ignore_dots and "dot" in node.type.lower()):
                                if node.name != "" and node.type != "":
                                    self.nodes.append(node)
                        node = Node()

    def fill_graph_from_script(self, ignore_backdrops=True):
        # "/mnt/x/PROJECTS/romulus/sequences/and/and_2000/comp/work/nuke/debug/and_2000_metawranglerTest.v002.nk"
        graph = {}  # {node_id: }
        node_id = 0
        node_lookup = {}
        buffer = ""
        nodes_raw_text = []
        window_layout_string = ""
        toggleBuffer = False

        def parse_raw_text(nuke_raw):
            nuke_raw = nuke_raw.replace("\n\n", "\n")
            type = nuke_raw.split(" ")[0]
            knobs = {}
            inputs = 1
            for line in nuke_raw.split(";"):
                line = line.strip()
                if line.startswith("inputs"):
                    inputs = int(line.split(" ")[-1].split("+")[0]) ### Figure out what exactly inputs like "4+1" mean, I assume its just "hanging" inputs that can be ignored.
                if line.endswith("}") and not line.startswith(type):
                    continue  # addUserKnob stuff
                knob_name = line.split(" ")[0]
                knob_args = " ".join(line.split(" ")[1:])
                knobs[knob_name] = knob_args
            if "name" in knobs:
                name = knobs["name"]
            else:
                name = ""
            return Node(name=name, type=type, knobs=knobs, num_inputs=inputs)

        def find_word_before_brace(text):
            if text.endswith("}\n") or text.endswith("scanline)\n"):
                return None
            pattern = r'\b\S+\b(?=\s\{)'
            match = re.search(pattern, text)
            return match.group(0) if match else None

        with open(self.script, "r") as f:
            got_version = False
            for n, line in enumerate(f.readlines()):
                node_found = find_word_before_brace(line)
                if line.strip().endswith("[stack 0]"):
                    nodes_raw_text.append(line+"; ")
                if line.strip().startswith("push "):
                    nodes_raw_text.append(line.strip()+"; ")
                if node_found:
                    current_node_type = node_found
                if line.startswith("version 1") and not got_version:
                    version_major = line.split(" ")[1]
                    version_minor = line.split(" ")[2]
                    got_version = True ### to avoid some node randomly starting with that string
                # elif line.startswith("define_window_layout_xml"):
                #     window_layout_string = line.split(" ")[0]
                #     buffer.append(line.split(" ")[1]+"\n")
                elif node_found:
                    buffer += line + "; "
                    toggleBuffer = True
                elif toggleBuffer and not line.startswith("}"):
                    buffer += line + "; "
                elif line.startswith("}"):
                    toggleBuffer = False
                    buffer += "}; "
                    nodes_raw_text.append(buffer)
                    buffer = ""
                    node_id += 1
        stack_instructions = []
        for node_raw in nodes_raw_text:
            if node_raw.startswith("define_"):
                continue
            if node_raw.startswith("set ") or node_raw.startswith("push "):
                node_raw = node_raw.strip().replace(";", "")
                stack_instructions.append({node_raw.split(" ")[0]: node_raw.split(" ")[1]})
                continue
            node = parse_raw_text(node_raw)
            print(node.type, "BackDrop" in node.type)
            if ignore_backdrops and "Backdrop" in node.type:
                continue
            stack_instructions.append({"place_node": node})
        self.construct_dag(stack_instructions, simplified=True)

    def construct_dag(self, stack_instructions, simplified=True):
        if simplified:
            for instruction in stack_instructions:
                if "place_node" in instruction:
                    self.simplifiedDAG.append(instruction["place_node"])
        else:
            # print(stack_instructions)
            while stack_instructions:
                next_instruction = stack_instructions.pop(0)
                self.execute_instruction(next_instruction)
                # print("Stack Status:", self.stack)


    def execute_instruction(self, instruction):
        if "set" in instruction.keys():
            self.stack.append(["$"+instruction['set'], [self.stack[-1][0]]]) ### Set checkpoint address to last node entry, "$" to signify its a reroute address
        if "push" in instruction.keys():
            if instruction['push'] == "0":
                self.stack.append(["", [""]]) ### Null pointer to "shield" instruction before it from pop()
            else:
                self.stack.append(["",[instruction['push']]]) ### Adds empty slot that points to address, to be filled with next node, will match the ones from the set instruction
        if 'place_node' in instruction.keys():
            node = instruction['place_node']
            if node.num_inputs == 0:
                if self.stack:
                    self.stack.pop(-1)
                self.stack.append([node.name, []]) ### If Node has no inputs, pop last entry from stack and add it as new root node
            if node.num_inputs > 0:
                list_in_nodes = []
                for inputs in range(1, node.num_inputs+1):
                    n = 1
                    while True:
                        stack_address = self.stack[-n][0]
                        if stack_address == "": ### instruction right after a push instruction attaches itself to the reroute address.
                            self.stack[-n][0] = node.name ### insert itself into the rerouting address
                            for in_address in self.stack[-n][1]: ### replace rerouting address with actual node address.
                                if in_address.startswith("$"):
                                    for reroute in self.stack:
                                        if reroute[0] == in_address:
                                            self.stack[-n][1] = [reroute[1][0]] ### can you go deeper with the nested loops, I'm not sure
                                            list_in_nodes.append(reroute[1][0])
                                            n += 1
                                            break

                        elif stack_address.startswith("$"): ### only use rerouting after push instruction
                            n += 1
                            break

                        else:
                            list_in_nodes.append(stack_address)
                            del self.stack[-n]
                            n += 1
                            break
                out_list = [node.name]
                out_list.append(list_in_nodes)
                self.stack.append(out_list)
                list_in_nodes = [] ### flush

    def find_path(self, graph, start, end, path=[]):
        path = path + [start]
        if start == end:
            return path
        if not graph.has_key(start):
            return None
        for node in graph[start]:
            if node not in path:
                newpath = self.find_path(graph, node, end, path)
                if newpath: return newpath
        return None

    def find_all_paths(self, graph, start, end, path=[]):
        path = path + [start]
        if start == end:
            return [path]
        if not graph.has_key(start):
            return []
        paths = []
        for node in graph[start]:
            if node not in path:
                newpaths = self.find_all_paths(graph, node, end, path)
                for newpath in newpaths:
                    paths.append(newpath)
        return paths

# graph = Graph("/mnt/x/PROJECTS/romulus/sequences/and/and_2000/comp/work/nuke/debug/and_2000_metawranglerTest.v011.nk")
# graph = Graph("/mnt/x/PROJECTS/romulus/sequences/wro/wro_6300/comp/work/nuke/Comp-WIP/wro_6300_debugDaniel.v001.nk")
# graph = Graph("/mnt/x/PROJECTS/romulus/sequences/wro/wro_1860/comp/work/nuke/Comp-WIP/wro_1860_test.v003.nk")
#
# for node in graph.simplifiedDAG:
#     if "Group" in node.type:
#         print(node)
# print(len(graph.simplifiedDAG))