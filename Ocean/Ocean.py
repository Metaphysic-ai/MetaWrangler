from graphlib import TopologicalSorter
import re

class Node:
    def __init__(self, name, type, knobs, num_inputs):
        self.name = name
        self.type = type
        self.knobs = knobs
        self.num_inputs = num_inputs
        self.root_node = None
        self.out_nodes = []
        self.in_nodes = []
    def __repr__(self):
        return f'Node(\'{self.type}\', {self.name})'

class NukeScript:
    def __init__(self):
        self.nodeGraph = Graph()

class Graph:
    def __init__(self, script):
        self.is_constructing = True
        self.graph_dict = {}
        self.script = script
        self.stack = []
        self.simplifiedDAG = []
        self.nodes = self.fill_graph_from_script()


    def fill_graph_from_script(self):
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