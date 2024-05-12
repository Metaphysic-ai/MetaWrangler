from sentence_transformers import SentenceTransformer, util
from sgt import SGT
import random
import torch
from sklearn.preprocessing import normalize
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt

from pathlib import Path
from llama_index.readers.file import PyMuPDFReader
import numpy as np

from Ocean import Graph

class VectorStore():

    def __init__(self):
        self.embed_model = SentenceTransformer("all-MiniLM-L6-v2")
        self.embed_model.max_seq_length = 256
        self.db = None
        self.node_lookup = {}
        self.dag_lookup = {}
        self.scripts = []
        self.script_embeddings = []

    def get_node_sequence(self, nodes):
        node_sequence = []
        for node in nodes:
            node_sequence.append(node.type)
        print(len(set(node_sequence)))
        return node_sequence

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
                graph = Graph(script_path, ignore_backdrops=True, ignore_dots=True)
                nodes = graph.nodes
                script_nodes.append(nodes)
                script_graphs.append(graph.simplifiedDAG)
                print(script_paths[n], graph.simplifiedDAG)
            print(script_nodes[save_index])
            print(script_paths[save_index])
            print(script_graphs[save_index])
            print("Total Graphs:", len(script_nodes), "nodes in v003:", len(script_nodes[save_index]), "unique nodes:", len(script_graphs[save_index]))
        else:
            pass
    def _bkp_get_vector_db(self, reload=True):

        if reload:
            print("Generating vector db.")
            docs = []
            script_paths = self.get_scripts_for_db()
            script_paths = random.sample(script_paths, 10)
            node_sequences = []
            for n, script_path in enumerate(script_paths):
                if n%1000 == 0:
                    print(f"Parsed {n}/{len(script_paths)} Scripts.")
                graph = Graph(script_path)
                nodes = graph.simplifiedDAG
                node_sequences.append(self.get_node_sequence(nodes))
                self.scripts.append(script_path)

            sgt = SGT(kappa=10, lengthsensitive=False, mode='multiprocessing')
            embedding = sgt.fit_transform(node_sequences)
            pca = PCA(n_components=2)
            pca.fit(embedding)
            X = pca.transform(embedding)
            print(np.sum(pca.explained_variance_ratio_))

            # kmeans = KMeans(n_clusters=3, max_iter=300)
            # kmeans.fit(X)
            # labels = kmeans.predict(X)
            # centroids = kmeans.cluster_centers_
            # fig = plt.figure(figsize=(5, 5))
            # colmap = {1: 'r', 2: 'g', 3: 'b'}
            # colors = list(map(lambda x: colmap[x + 1], labels))
            # plt.scatter(df['x1'], df['x2'], color=colors, alpha=0.5, edgecolor=colors)

                #docs.append(doc)

            # db = FAISS.from_documents(docs, self.embeddings)
            # self.dag_lookup
            # query_script = "/mnt/x/PROJECTS/romulus/sequences/etx/etx_9800/comp/work/nuke/Plate-Denoise/etx_9800_bg02Denoise.v003.nk"
            # query_embedding = self.embedding_from_nodes(Graph(query_script).simplifiedDAG)

            # dot_scores = util.dot_score(query_embedding, self.script_embeddings)[0]
            # top_results = torch.topk(dot_scores, k=5)

            # for score, idx in zip(top_results[0], top_results[1]):
            #     print(self.scripts[idx], "(Score: {:.4f})".format(score))



        else:
            print("Loading existing db.")
            db = FAISS.load_local("faiss_index", self.embeddings, allow_dangerous_deserialization=True)

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

        graph = Graph(script)
        nodes = graph.simplifiedDAG
        docs = retriever.invoke(self.naive_script_text(nodes, script).page_content)
        for doc in docs:
            print(doc.dict()["metadata"]["path"])

    def run(self):
        self.get_vector_db(reload=True)

vs = VectorStore()
vs.run()