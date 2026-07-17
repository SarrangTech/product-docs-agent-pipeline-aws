import boto3, gzip, json, math, os, time

BUCKET = os.environ.get("BUCKET", "sarang-lake-bronze")
GOLD_PREFIX = "source=github/entity=docs-gold/"

def load_gold_chunks(s3):
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=GOLD_PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".jsonl.gz"):
                keys.append(obj["Key"])
    if not keys:
        raise ValueError("No gold files found")
    latest = sorted(keys)[-1]
    obj = s3.get_object(Bucket=BUCKET, Key=latest)
    raw = gzip.decompress(obj["Body"].read()).decode("utf-8")
    return [json.loads(line) for line in raw.strip().split("\n")]

def get_embedding(bedrock, text):
    body = json.dumps({"inputText": text[:8000]})
    resp = bedrock.invoke_model(
        modelId="amazon.titan-embed-text-v2:0",
        body=body,
        contentType="application/json",
        accept="application/json"
    )
    return json.loads(resp["body"].read())["embedding"]

def cosine_similarity(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)

# cache chunks across Lambda invocations
_chunks_cache = None

def lambda_handler(event, context):
    global _chunks_cache

    body = event.get("body", "{}")
    if isinstance(body, str):
        body = json.loads(body)

    query = body.get("query", "").strip()
    top_k = int(body.get("top_k", 5))
    section = body.get("section", None)

    if not query or len(query) < 2:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "query must be at least 2 characters"})
        }

    top_k = min(max(top_k, 1), 20)

    s3 = boto3.client("s3", region_name="us-east-1")
    bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")

    t0 = time.monotonic()

    if _chunks_cache is None:
        print("Loading gold chunks from S3...")
        _chunks_cache = load_gold_chunks(s3)
    chunks = _chunks_cache

    if section:
        chunks = [c for c in chunks if c["doc_path"].startswith(section)]

    query_embedding = get_embedding(bedrock, query)
    t1 = time.monotonic()

    scored = []
    for chunk in chunks:
        sim = cosine_similarity(query_embedding, chunk["embedding"])
        scored.append({
            "chunk_id": chunk["chunk_id"],
            "doc_path": chunk["doc_path"],
            "title": chunk.get("title"),
            "summary": chunk.get("summary"),
            "chunk_text": chunk["chunk_text"],
            "score": round(sim, 4),
        })

    scored.sort(key=lambda x: x["score"], reverse=True)
    results = scored[:top_k]

    t2 = time.monotonic()

    return {
        "statusCode": 200,
        "body": json.dumps({
            "chunks": results,
            "total_chunks_searched": len(chunks),
            "embed_and_search_ms": int((t2 - t0) * 1000),
            "query": query,
        })
    }
