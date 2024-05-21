from sentence_transformers import SentenceTransformer, util
import re
import numpy as np
from sklearn.decomposition import TruncatedSVD
from sklearn.metrics.pairwise import cosine_similarity

class NukeScript():
    def __init__(self, script_path):
        self.script_path = script_path
        self.write_nodes = {}
        self.write_node_embeddings = {}
        self.job_embeddings = {}
        self.write_dependency_dict = {}

class Node():
    def __init(self):
        self.name = None
        self.embedding = None
        self.weight = None

class VectorStoreUtils():

    def __init__(self):
        self.embed_model = SentenceTransformer("all-MiniLM-L6-v2")
        self.embed_model.max_seq_length = 256
        self.db = None
        self.node_lookup = {}
        self.dag_lookup = {}
        self.scripts_to_parse = []
        self.script_embeddings = []

    def vectorize(self, nuke_script):
        write_node_keys = nuke_script.write_nodes.keys()
        for write_node in write_node_keys:
            nuke_script.write_node_embeddings[write_node] = {}
            nodes = []
            for node_name in nuke_script.write_nodes[write_node]:
                node = Node()
                node.name = node_name
                nodes.append(node)
            node_name_embeddings = self.embed_model.encode([node.name for node in nodes])
            for node, embedding in zip(nodes, node_name_embeddings):
                nuke_script.write_node_embeddings[write_node][node.name] = embedding
        self.weighted_average(nuke_script)
        self.adjust_for_pca(nuke_script)
        query_embedding = []
        rest_embeddings = {}
        for k, v in nuke_script.job_embeddings.items():
            if k == "ShotGridWrite15":
                query_embedding = v
            else:
                rest_embeddings[k] = v
                print(k, v.shape, v[:5])
        similarities, keys = self.find_most_similar(query_embedding, rest_embeddings)
        paired = list(zip(similarities, keys))
        paired_sorted = sorted(paired, key=lambda x: x[0])
        sorted_similarity_scores, sorted_keys = zip(*paired_sorted)


    def weighted_average(self, nuke_script):
        write_node_keys = nuke_script.write_nodes.keys()
        for write_node in write_node_keys:
            graph_dict = nuke_script.write_nodes[write_node]
            nuke_script.job_embeddings[write_node] = {}

            a = 0.001
            total_node_num = sum(graph_dict.values())
            word_frequencies = {word: freq / total_node_num for word, freq in graph_dict.items()}

            embedding_matrix = np.zeros((len(graph_dict), 384))
            weights = []

            for i, (node_name, freq) in enumerate(graph_dict.items()):
                if node_name in nuke_script.write_node_embeddings[write_node]:
                    embedding_matrix[i] = nuke_script.write_node_embeddings[write_node][node_name]
                    weights.append(a / (a + word_frequencies[node_name]))

            weights = np.array(weights)
            nuke_script.job_embeddings[write_node] = np.dot(weights, embedding_matrix) / np.sum(weights)

    def adjust_for_pca(self, nuke_script):
        write_node_keys = nuke_script.write_nodes.keys()
        write_nodes = []
        adjusted_embeddings = []
        for write_node in write_node_keys:
            write_nodes.append(write_node)
            adjusted_embeddings.append(nuke_script.job_embeddings[write_node])
        adjusted_embeddings = np.array(adjusted_embeddings)
        adjusted_embeddings = self.remove_pc(self.compute_pc(adjusted_embeddings))
        for write_node, adjusted_embedding in zip(write_nodes, adjusted_embeddings):
            nuke_script.job_embeddings[write_node] = adjusted_embedding

    def compute_pc(self, X, npc=1):
        svd = TruncatedSVD(n_components=npc, n_iter=7, random_state=0)
        svd.fit(X)
        return svd.components_

    def remove_pc(self, X, npc=1):
        pc = self.compute_pc(X, npc)
        if npc == 1:
            XX = X - X.dot(pc.transpose()) * pc
        else:
            XX = X - X.dot(pc.transpose()).dot(pc)
        return XX

    def compute_cosine_similarity(self, vec1, vec2):
        vec1 = vec1.reshape(1, -1)
        vec2 = vec2.reshape(1, -1)
        return cosine_similarity(vec1, vec2)[0][0]

    def find_most_similar(self, query_embedding, all_nodes):
        max_similar = -1
        similar_key = ""
        similarities =

        for key, embedding in all_nodes.items():
            similarity = self.compute_cosine_similarity(query_embedding, embedding)
            if similarity > max_similar:
                max_similar = similarity
                similar_key = key

        return max_similar, similar_key

    def get_node_sequence(self, nodes):
        node_sequence = []
        for node in nodes:
            node_sequence.append(node.type)
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
        nuke_script = None
        for script, write_dependencies in dependency_dict.items():
            nuke_script = NukeScript(script)
            nuke_script.write_dependency_dict = dependency_dict
            for write_node in write_dependencies.keys():
                nodes_accumulated = self.accumulate_nodes(write_dependencies[write_node])
                nuke_script.write_nodes[write_node] = nodes_accumulated
            print("Script:", nuke_script.script_path)
            # print("Dependencies:")
            # print(nuke_script.write_dependency_dict)
            print("Accumulated:")
            for k, v in nuke_script.write_nodes.items():
                print(k, v)
        return nuke_script

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