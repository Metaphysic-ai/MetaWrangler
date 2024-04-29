from sentence_transformers import SentenceTransformer
from langchain_community.vectorstores import FAISS
from langchain.docstore.document import Document
from langchain_community.embeddings import HuggingFaceEmbeddings
from llama_index.embeddings.openai import OpenAIEmbedding

from pathlib import Path
from llama_index.readers.file import PyMuPDFReader
import numpy as np

from Ocean import Graph

class VectorStore():

    def __init__(self):
        self.model = SentenceTransformer('BAAI/bge-large-en')
        self.embeddings = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")
        self.embed_model = OpenAIEmbedding()

    def embedding_from_nodes(self, nodes):
        embeddings_all = []
        for node in nodes:
            node_embedding = self.embed_model.get_text_embedding(
                node.name
            )
            embeddings_all.append(np.array(node_embedding))
        embeddings_all = np.asarray(embeddings_all)
        return np.mean(embeddings_all, axis=0)

    def naive_script_text(self, nodes, script):
        metadata = {}
        page_content = ""
        metadata["path"] = script
        for node in nodes:
            page_content += node.type+": "+node.name+"\n"
        return Document(page_content=page_content, metadata=metadata)



    def get_vector_db(self, reload=True):

        if reload:
            print("Generating vector db.")
            docs = []
            script_paths = self.get_scripts_for_db()
            for n, script_path in enumerate(script_paths):
                if n % (int(len(script_paths) / 20)) == 0:
                    print(f"Parsed {n}/{len(script_paths)} Scripts.")
                graph = Graph(script_path)
                nodes = graph.simplifiedDAG

                doc = self.naive_script_text(nodes, script_path)
                # em = self.embedding_from_nodes(nodes)

                docs.append(doc)

            db = FAISS.from_documents(docs, self.embeddings)
            print(f"Created {len(docs)} embeddings.")
            db.save_local("faiss_index")

        else:
            print("Loading existing db.")
            db = FAISS.load_local("faiss_index", self.embeddings, allow_dangerous_deserialization=True)

        return db

    def get_similar(self, db):
        pass

    def get_scripts_for_db(self):
        print("Getting scripts files.")
        import os
        out_scripts_files = []
        directories = [
            "/mnt/x/PROJECTS/romulus/sequences/and/and_2000/comp/work/nuke/debug/",
            "/mnt/x/PROJECTS/romulus/sequences/wro/wro_6300/comp/work/nuke/Comp-WIP/"
        ]
        for directory in directories:
            for file in os.listdir(directory):
                if file.endswith(".nk"):
                    out_scripts_files.append(os.path.join(directory, file))
        return out_scripts_files

    def run(self):
        db = self.get_vector_db(reload=False)

        retriever = db.as_retriever(search_type='similarity', search_kwargs={'k': 5})
        # docs = self.db.max_marginal_relevance_search (query, k=5)

        test_script_path = "/mnt/x/PROJECTS/romulus/sequences/and/and_2000/comp/work/nuke/debug/and_2000_metawranglerTest.v011.nk"
        graph = Graph(test_script_path)
        nodes = graph.simplifiedDAG
        docs = retriever.invoke(self.naive_script_text(nodes, test_script_path).page_content)
        for doc in docs:
            print(doc.dict()["metadata"]["path"])





vs = VectorStore()
vs.run()