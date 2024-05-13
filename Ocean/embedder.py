from sentence_transformers import SentenceTransformer, util
import re
# from sgt import SGT
# import random
# import torch
# from sklearn.preprocessing import normalize
# from sklearn.decomposition import PCA
# from sklearn.cluster import KMeans
# import matplotlib.pyplot as plt
#
# from pathlib import Path
# from llama_index.readers.file import PyMuPDFReader
# import numpy as np

class NukeScript():
    def __init__(self, script_path):
        self.script_path = script_path
        self.write_nodes = {}
        self.write_dependency_dict = {}
class VectorStoreUtils():

    def __init__(self):
        self.embed_model = SentenceTransformer("all-MiniLM-L6-v2")
        self.embed_model.max_seq_length = 256
        self.db = None
        self.node_lookup = {}
        self.dag_lookup = {}
        self.scripts_to_parse = []
        self.script_embeddings = []

    def get_node_sequence(self, nodes):
        node_sequence = []
        for node in nodes:
            node_sequence.append(node.type)
        print(len(set(node_sequence)))
        return node_sequence

    def accumulate_nodes(self, nodes):
        def strip_trailing_digits_and_underscores(s):
            return re.sub(r'_*\d+$', '', s)
        # sort nodes by node type and count their occurences
        nodes_accum = {}
        for node in nodes:
            node_type, node_name = node
            if 'Group' not in node_type and 'RawPred' not in node_name: # Groups and special RawPredAlignment get counted separately
                if node_type not in nodes_accum:
                    nodes_accum[node_type] = 1
                else:
                    nodes_accum[node_type] += 1
            else:
                if node_name not in nodes_accum: # If it's a group or another special node, use the group name as identifier
                    nodes_accum[strip_trailing_digits_and_underscores(node_name)] = 1
                else:
                    nodes_accum[strip_trailing_digits_and_underscores(node_name)] += 1
        return nodes_accum

    def parse_dependency_dict(self, dependency_dict):
        for script, write_dependencies in dependency_dict.items():
            nuke_script = NukeScript(script)
            nuke_script.write_dependency_dict = dependency_dict
            for write_node in write_dependencies.keys():
                nodes_accumulated = self.accumulate_nodes(write_dependencies[write_node])
                nuke_script.write_nodes[write_node] = nodes_accumulated
            print("Script:", nuke_script.script_path)
            print("Dependencies:")
            print(nuke_script.write_dependency_dict)
            print("Accumulated:")
            print(nuke_script.write_nodes)
            # self.scripts_to_parse.append(nuke_script)

    def get_vector_db(self, reload=True):
        if reload:
            print("Generating vector db.")
            script_paths = self.get_scripts_for_db()
            script_nodes = []
            script_graphs = []
            save_index = 0
            for n, script_path in enumerate(script_paths):
                if "wro_1860_MetaPiPRomUnitTest.v003.nk" in script_path:
                    save_index = n
                # graph = Graph(script_path, ignore_backdrops=True, ignore_dots=True)
                # nodes = graph.nodes
                # script_nodes.append(nodes)
                # script_graphs.append(graph.simplifiedDAG)
                # print(script_paths[n], graph.simplifiedDAG)
            print(script_nodes[save_index])
            print(script_paths[save_index])
            print(script_graphs[save_index])
            print("Total Graphs:", len(script_nodes), "nodes in v003:", len(script_nodes[save_index]), "unique nodes:", len(script_graphs[save_index]))
        else:
            pass

    def get_similar(self, db):
        pass

    def get_scripts_for_db(self):
        import random
        print("Getting script files.")
        import os
        out_scripts_files = []
        # root_directories = [
        #     "/mnt/x/PROJECTS/romulus/sequences",
        #     "/mnt/x/PROJECTS/houdini/sequences",
        #     "/mnt/x/PROJECTS/peach/sequences",
        #     "/mnt/x/PROJECTS/ick/sequences",
        # ]

        # Compare scripts:
        # /mnt/x/PROJECTS/romulus/sequences/etx/etx_9800/comp/work/nuke/Plate-Denoise/etx_9800_bg02Denoise.v003.nk
        #


        root_directories = [
            "/mnt/x/PROJECTS/romulus/sequences/wro/wro_1860/comp/work/nuke/Comp-WIP/",
            "/mnt/x/PROJECTS/pipeline/sequences/ABC/ABC_0000/comp/work/nuke/MetaWranglerUnitTest/",
        ]
        count = 0
        for root_dir in root_directories:
            print("Parsing", root_dir)
            for dirpath, dirnames, filenames in os.walk(root_dir):
                for filename in filenames:
                    if filename.endswith(".nk"):
                        full_path = os.path.join(dirpath, filename)
                        out_scripts_files.append(full_path)
                        count += 1
                        if count % 1000 == 0:
                            print(count)
        return out_scripts_files

    def get_similar_script(self, script):
        # "/mnt/x/PROJECTS/romulus/sequences/and/and_2000/comp/work/nuke/debug/and_2000_metawranglerTest.v011.nk"
        retriever = self.db.as_retriever(search_type='similarity', search_kwargs={'k': 5})
        # docs = self.db.max_marginal_relevance_search (query, k=5)

    def run(self):
        self.get_vector_db(reload=True)