import boto3, gzip, json, re, datetime

BUCKET = "sarang-lake-bronze"
BRONZE_PREFIX = "source=github/entity=docs/"
SILVER_PREFIX = "source=github/entity=docs-silver/"

def parse_frontmatter(content):
    title_match = re.search(r"^#\s+(.+?)$", content, re.MULTILINE)
    title = title_match.group(1).strip() if title_match else None
    return title, None

def clean_body(content):
    body = re.sub(r"```[\s\S]*?```", "", content)
    body = re.sub(r"<[^>]+>", "", body)
    body = re.sub(r"\n{3,}", "\n\n", body).strip()
    return body

def get_latest_bronze_key(s3):
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=BRONZE_PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".jsonl.gz"):
                keys.append(obj["Key"])
    if not keys:
        raise ValueError("No bronze files found")
    return sorted(keys)[-1]

def refine_silver():
    s3 = boto3.client("s3", region_name="us-east-1")
    today = datetime.date.today().isoformat()
    refined_at = datetime.datetime.now(datetime.UTC).isoformat()
    print("Finding latest bronze file...")
    bronze_key = get_latest_bronze_key(s3)
    print(f"Reading: {bronze_key}")
    obj = s3.get_object(Bucket=BUCKET, Key=bronze_key)
    raw = gzip.decompress(obj["Body"].read()).decode("utf-8")
    records = [json.loads(line) for line in raw.strip().split("\n")]
    print(f"Bronze records: {len(records)}")
    silver_records = []
    for r in records:
        content = r.get("content", "")
        if not content:
            continue
        title, summary = parse_frontmatter(content)
        body_for_embed = clean_body(content)
        silver_records.append({
            "doc_path": r["doc_path"],
            "title": title,
            "summary": summary,
            "body_for_embed": body_for_embed,
            "body_full": content,
            "content_hash": r["content_hash"],
            "_commit": r["_commit"],
            "_pulled_at": r["_pulled_at"],
            "_refined_at": refined_at,
        })
    seen = {}
    for rec in silver_records:
        path = rec["doc_path"]
        if path not in seen or rec["_pulled_at"] > seen[path]["_pulled_at"]:
            seen[path] = rec
    silver_records = list(seen.values())
    print(f"Silver records after dedup: {len(silver_records)}")
    body = "\n".join(json.dumps(r) for r in silver_records).encode("utf-8")
    compressed = gzip.compress(body)
    silver_key = f"{SILVER_PREFIX}ingestion_date={today}/silver_docs.jsonl.gz"
    s3.put_object(Bucket=BUCKET, Key=silver_key, Body=compressed, ContentType="application/gzip")
    print(f"Written to s3://{BUCKET}/{silver_key}")
    print(f"Records: {len(silver_records)}")
    print(f"Size: {len(compressed)/1024:.1f} KB")

if __name__ == "__main__":
    refine_silver()
