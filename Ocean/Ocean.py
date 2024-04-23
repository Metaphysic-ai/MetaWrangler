from graphlib import TopologicalSorter
import re

class Node:
    def __init__(self, name, type, knobs, num_inputs):
        self.name = name
        self.type = type
        self.knobs = knobs
        self.num_inputs = num_inputs
        self.root_node = None
        self.outputs = [[]]

class NukeScript:
    def __init__(self):
        self.nodeGraph = Graph()

class Graph:
    def __init__(self, script):
        self.script = script
        self.window_layout = {}
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
            inputs = None
            for line in nuke_raw.split(";"):
                line = line.strip()
                if line.startswith("inputs"):
                    inputs = line.split(" ")[-1]
                if line.endswith("}") and not line.startswith(type):
                    continue  # addUserKnob stuff
                knob_name = line.split(" ")[0]
                knob_args = " ".join(line.split(" ")[1:])
                knobs[knob_name] = knob_args
            name = knobs["name"]
            return Node(name=name, type=type, knobs=knobs, num_inputs=inputs)

        def find_word_before_brace(text):
            if text.endswith("}\n") or text.endswith("scanline)\n"):
                return None
            pattern = r'\b\S+\b(?=\s\{)'
            match = re.search(pattern, text)
            return match.group(0) if match else None

        with open(self.script, "r") as f:
            for n, line in enumerate(f.readlines()):
                node_found = find_word_before_brace(line)
                if line.strip().endswith("[stack 0]"):
                    nodes_raw_text.append(line+"; ")
                if line.strip().startswith("push "):
                    nodes_raw_text.append(line.strip()+"; ")
                if node_found:
                    current_node_type = node_found
                if line.startswith("version 1"):
                    version_major = line.split(" ")[1]
                    version_minor = line.split(" ")[2]
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
        self.construct_stack(stack_instructions)

    def construct_stack(self, stack_instructions):
        # for instruction in stack_instructions:
        #     if 'place_node' in instruction:
        #         for k, v in instruction.items():
        #             print(k, v.name)
        #     else:
        #         for k, v in instruction.items():
        #             print(k, v)
        # stack = {} # {NodeName: {output_key: [outputs]}}
        # saved_heads = {"DEFAULT": []}
        # head_stack = ["DEFAULT"]
        # previous_node = {"name": ""}
        # node_lookup = {}
        # for n, instruction in enumerate(stack_instructions):
        #     print("###########")
        #     if "place_node" in instruction:
        #         print(instruction["place_node"].name)
        #     else:
        #         print(instruction)
        #     print(stack)
        #     print(saved_heads)
        #     print(head_stack)
        #     print(previous_node)
        #     if 'set' in instruction:
        #         saved_heads[instruction["set"]].append(previous_node.name)
        #         head_stack.append(instruction["set"])
        #     if 'push' in instruction:
        #         if instruction["push"][1:] in saved_heads:
        #             del saved_heads[instruction["push"][1:]]
        #             head_stack.pop()
        #     if 'place_node' in instruction:
        #         if "node" in locals():
        #             previous_node = node
        #         node = instruction["place_node"]
        #         node_lookup[node.name] = node
        #         if node.num_inputs == "0":
        #             inputs = 0
        #             stack[node.name] = {"DEFAULT": []}
        #             saved_heads["DEFAULT"].append(node.name)
        #             node.root_node = node.name
        #         if node.num_inputs is None:
        #             inputs = 1
        #         else:
        #             inputs = int(node.num_inputs)
        #         for input in range(inputs):
        #             head_node = saved_heads[head_stack[-1]][-1-input]
        #             node.root_node = node_lookup[head_node].root_node
        #             stack[node.root_node][head_stack[-1]].append(node.name)
        #         saved_heads[head_stack[-1]].append(node.name)


        stack = {}
        heads = [] # [{head_index: head}]
        multi_inputs = [0]
        previous_node = ""
        triggerMerge = False
        checkpoints = []
        num_checkpoints = 0
        for n, instruction in enumerate(stack_instructions):

            if 'set' in instruction:
                multi_inputs.append(len(heads))
                triggerMerge = True
                num_checkpoints += 1
            if 'push' in instruction:
                multi_inputs.pop()
                checkpoints.pop()
                num_checkpoints -= 1
            if 'place_node' in instruction:
                if "node" in locals():
                    previous_node = node
                node = instruction["place_node"]
                if node.type == "Root":
                    stack[node.name] = [[]]
                elif n != 0:

                    if node.num_inputs == "0":
                        stack[node.name] = [[]]
                        heads.append({"root": node.name, "head": node.name})
                        multi_inputs[0] = len(heads)-1
                    else:
                        if node.num_inputs is None:
                            _inputs = 1
                        else:
                            _inputs = int(node.num_inputs)
                        clear_heads = True if _inputs > 1 else False
                        for input in range(_inputs):
                            if _inputs > 1:
                                for n_multi, multi_index in enumerate(multi_inputs):
                                    if node.name not in stack:
                                        stack[node.name] = [[]]
                                    heads[-1 - input]["head"] = node.name
                                    if n_multi > 0:
                                        stack[heads[-1 - input]["root"]].append([])
                                    stack[heads[-1 - input]["root"]][n_multi].extend([node.name])
                            # heads.append({"root": heads[-1]["root"], "head": node.name})
                            else:
                                if len(multi_inputs) == 1:
                                    heads[-1-input]["head"] = node.name
                                    # stack[heads[-1 - input]["root"]].append([])
                                    stack[heads[-1-input]["root"]][-1].extend([node.name])
                                else:
                                    # heads.append({'root': heads[-1-input]["root"], "head": node.name})
                                    # heads.append({'root': heads[multi_inputs[n_multi]]["head"], "head": node.name})
                                    if triggerMerge:
                                        count_equal = {}
                                        for head in heads:
                                            if not head["head"] in count_equal:
                                                count_equal[head["head"]] = 0
                                            else:
                                                count_equal[head["head"]] += 1
                                        for _ in range(_inputs):
                                            heads.pop()
                                        heads.append({'root': previous_node.name, "head": previous_node.name})
                                        checkpoints.append(heads[-1])
                                        triggerMerge = False
                                    found = False
                                    added = False
                                    for n, endpoint in enumerate(stack[heads[-1 - input]["root"]]):
                                        if heads[-1-input]["head"] in endpoint:
                                            found = True
                                            stack[heads[-1 - input]["root"]][n].extend([node.name])
                                            added = True
                                    if not found:
                                        stack[heads[-1 - input]["root"]][-1-input].extend([node.name])
                                        added = True
                                    if checkpoints:
                                        if len(stack[checkpoints[-1]["head"]]) < len(multi_inputs):
                                            stack[checkpoints[-1]["head"]].append([])
                                            heads.append({'root': checkpoints[-1]["root"], 'head': node.name})
                                            continue
                                        if num_checkpoints > 0 and not added:
                                            stack[checkpoints[-1]["head"]][num_checkpoints].extend([node.name])
                                    # for n_multi, multi_index in enumerate(multi_inputs):
                                    #     if len(stack[heads[-1 - n_multi]["head"]]) < len(multi_inputs):
                                    #         stack[heads[-1 - n_multi]["head"]].append([])


        for k, v in stack.items():
            print("####", k, "outputs:", v)

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

graph = Graph("/mnt/x/PROJECTS/romulus/sequences/and/and_2000/comp/work/nuke/debug/and_2000_metawranglerTest.v005.nk")