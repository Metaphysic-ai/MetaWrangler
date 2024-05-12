from sentence_transformers import SentenceTransformer, util

model = SentenceTransformer("all-MiniLM-L6-v2")
print(model.max_seq_length)

model.max_seq_length = 256

import torch

# Corpus with example sentences
corpus = ['A man is eating food.',
          'A man is eating a piece of bread.',
          'The girl is carrying a baby.',
          'A man is riding a horse.',
          'A woman is playing violin.',
          'Two men pushed carts through the woods.',
          'A man is riding a white horse on an enclosed ground.',
          'A monkey is playing drums.',
          'A cheetah is running behind its prey.'
          ]
corpus_embeddings = model.encode(corpus, normalize_embeddings=True)

query = "What is the man eating?"
query_embedding = model.encode(query, normalize_embeddings=True)

# Since the embeddings are normalized, we can use dot_score to find the highest 5 scores
dot_scores = util.dot_score(query_embedding, corpus_embeddings)[0]
top_results = torch.topk(dot_scores, k=5)

for score, idx in zip(top_results[0], top_results[1]):
    print(corpus[idx], "(Score: {:.4f})".format(score))