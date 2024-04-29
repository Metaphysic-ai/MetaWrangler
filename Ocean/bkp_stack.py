stack = {}
        # heads = [] # [{head_index: head}]
        # multi_inputs = [0]
        # previous_node = ""
        # triggerMerge = False
        # checkpoints = {"0": {"root:": "Root", "head:": "Root"}}
        # next_checkpoint = {}
        # for n, instruction in enumerate(stack_instructions):
        #     print(instruction)
        #
        #     if 'set' in instruction:
        #         multi_inputs.append(len(heads))
        #         checkpoints.append(heads[-1])
        #         triggerMerge = True
        #         index_checkpoints.append(len(checkpoints))
        #     if 'push' in instruction:
        #         next_checkpoint = checkpoints.pop()
        #         index_checkpoints.pop()
        #     if 'place_node' in instruction:
        #         if "node" in locals():
        #             previous_node = node
        #         node = instruction["place_node"]
        #         if node.type == "Root":
        #             stack[node.name] = [[]]
        #         elif n != 0:
        #
        #             if node.num_inputs == "0":
        #                 stack[node.name] = [[]]
        #                 heads.append({"root": node.name, "head": node.name})
        #                 multi_inputs[0] = len(heads)-1
        #             else:
        #                 if node.num_inputs is None:
        #                     _inputs = 1
        #                 else:
        #                     _inputs = int(node.num_inputs)
        #                 clear_heads = True if _inputs > 1 else False
        #                 for input in range(_inputs):
        #                     if _inputs > 1:
        #                         for n_multi, multi_index in enumerate(multi_inputs):
        #                             if node.name not in stack:
        #                                 stack[node.name] = [[]]
        #                             heads[-1 - input]["head"] = node.name
        #                             if n_multi > 0:
        #                                 stack[heads[-1 - input]["root"]].append([])
        #                             stack[heads[-1 - input]["root"]][n_multi].extend([node.name])
        #                     else:
        #                         if len(multi_inputs) == 1:
        #                             heads[-1-input]["head"] = node.name
        #                             # stack[heads[-1 - input]["root"]].append([])
        #                             stack[heads[-1-input]["root"]][-1].extend([node.name])
        #                         else:
        #                             if triggerMerge:
        #                                 count_equal = {}
        #                                 for head in heads:
        #                                     if not head["head"] in count_equal:
        #                                         count_equal[head["head"]] = 0
        #                                     else:
        #                                         count_equal[head["head"]] += 1
        #                                 for _ in range(_inputs):
        #                                     heads.pop()
        #                                 heads.append({'root': previous_node.name, "head": previous_node.name})
        #                                 checkpoints.append(heads[-1])
        #                                 triggerMerge = False
        #                             found = False
        #                             added = False
        #                             for n, endpoint in enumerate(stack[heads[-1 - input]["root"]]):
        #                                 if heads[-1-input]["head"] in endpoint:
        #                                     found = True
        #                                     stack[heads[-1 - input]["root"]][n].extend([node.name])
        #                                     added = True
        #                             if not found:
        #                                 stack[heads[-1 - input]["root"]][-1-input].extend([node.name])
        #                                 added = True
        #                             if checkpoints:
        #                                 if len(stack[checkpoints[-1]["head"]]) < len(multi_inputs):
        #                                     stack[checkpoints[-1]["head"]].append([])
        #                                     heads.append({'root': checkpoints[-1]["root"], 'head': node.name})
        #                                     continue
        #                                 if len(index_checkpoints) > 0 and not added:
        #                                     stack[checkpoints[-1]["head"]][index_checkpoints].extend([node.name])
        #
        # for k, v in stack.items():
        #     print("####", k, "outputs:", v)