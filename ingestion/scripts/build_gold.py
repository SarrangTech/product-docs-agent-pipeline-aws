import boto3, gzip, json, re, datetime, hashlib

BUCKET = "sarang-lake-bronze"
SILVER_PREFIX = "source=github/entity=docs-silver/"
GOLD_PREFIX = "source=github/entity=docs-gold/"
CHUNK_SIZE = 2000
CHUNK_OVERLAP = 200

def chunk_text(text, size=CHUNK_SIZE, overlap=CHUNK_OVERLAP):
    chunks = []
    start = 0
    while start < len(text):
        end = start + size
        chunk = text[start:end]
        if len(chunk) > 200:
            chunks.append((start, chunk))
        start += size - overlap
    return chunks

def get_embedding(bedrock, text):
    body = json.dumps({"inputText": text[:8000]})
    resp = bedrock.invoke_model(
        modelId="amazon.titan-embed-text-v2:0",
        body=body,
        contentType="application/json",
        accept="application/json"
    )
    result = json.loads(resp["body"].read())
    return result["embedding"]

def get_latest_silver_key(s3):
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=SILVER_PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".jsonl.gz"):
                keys.append(obj["Key"])
    if not keys:
        raise ValueError("No silver files found")
    return sorted(keys)[-1]

def build_gold():
    s3 = boto3.client("s3", region_name="us-east-1")
    bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")
    today = datetime.date.today().isoformat()
    chunked_at = datetime.datetime.now(datetime.UTC).isoformat()

    print("Finding latest silver file...")
    silver_key = get_latest_silver_key(s3)
    print(f"Reading: {silver_key}")

    obj = s3.get_object(Bucket=BUCKET, Key=silver_key)
    raw = gzip.decompress(obj["Body"].read()).decode("utf-8")
    records = [json.loads(line) for line in raw.strip().split("\n")]
    print(f"Silver records: {len(records)}")

    gold_records = []
    for r in records:
        body = r.get("body_for_embed", "")
        if not body:
            continue
        chunks = chunk_text(body)
        print(f"  {r['doc_path']} -> {len(chunks)} chunks")
        for offset, text in chunks:
            chunk_id = hashlib.sha256(
                f"{r['doc_path']}::{offset}".encode()
            ).hexdigest()[:16]
            print(f"    Embedding chunk at offset {offset}...")
            embedding = get_embedding(bedrock, text)
            gold_records.append({
                "chunk_id": chunk_id,
                "doc_path": r["doc_path"],
                "title": r["title"],
                "summary": r["summary"],
                "chunk_offset": offset,
                "chunk_text": text,
                "embedding": embedding,
                "embedding_model": "amazon.titan-embed-text-v2:0",
                "content_hash": r["content_hash"],
                "_commit": r["_commit"],
                "_chunked_at": chunked_at,
            })

    print(f"Total gold chunks: {len(gold_records)}")
    body = "\n".join(json.dumps(r) for r in gold_records).encode("utf-8")
    compressed = gzip.compress(body)
    gold_key = f"{GOLD_PREFIX}ingestion_date={today}/gold_docs_chunks.jsonl.gz"
    s3.put_object(
        Bucket=BUCKET,
        Key=gold_key,
        Body=compressed,
        ContentType="application/gzip"
    )
    print(f"Written to s3://{BUCKET}/{gold_key}")
    print(f"Size: {len(compressed)/1024:.1f} KB")

if __name__ == "__main__":
    build_gold()
