import nuke
import sys
import os
script_path = sys.argv[0]
print(script_path)
os.environ["NUKE_INTERACTIVE"] = "1"
print(nuke.scriptOpen(script_path))

def send_dependency_graph():
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

send_dependency_graph()