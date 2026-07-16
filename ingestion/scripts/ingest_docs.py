import os, json, hashlib, subprocess, datetime, gzip, tempfile, pathlib
import boto3

REPO = "https://github.com/aws/aws-sdk-pandas.git"
BUCKET = "sarang-lake-bronze"

def ingest_docs():
    today = datetime.date.today().isoformat()
    pulled_at = datetime.datetime.utcnow().isoformat() + "Z"

    with tempfile.TemporaryDirectory() as tmp:
        print("Cloning repo...")
        subprocess.run(
            ["git", "clone", "--depth", "1", REPO, tmp],
            check=True, capture_output=True
        )
        commit = subprocess.check_output(
            ["git", "-C", tmp, "rev-parse", "HEAD"]
        ).decode().strip()
        print(f"Commit: {commit}")

        records = []
        for path in pathlib.Path(tmp).rglob("*.md"):
            text = path.read_text(encoding="utf-8", errors="ignore")
            rel = str(path.relative_to(tmp))
            records.append({
                "doc_path": rel,
                "content": text,
                "content_hash": hashlib.sha256(text.encode()).hexdigest(),
                "_repo": REPO,
                "_commit": commit,
                "_pulled_at": pulled_at,
            })

        print(f"Found {len(records)} markdown files")

    s3 = boto3.client("s3")
    blob_path = (
        f"source=github/entity=docs/"
        f"ingestion_date={today}/"
        f"commit={commit[:8]}/docs.jsonl.gz"
    )

    body = "\n".join(json.dumps(r) for r in records).encode("utf-8")
    compressed = gzip.compress(body)

    s3.put_object(
        Bucket=BUCKET,
        Key=blob_path,
        Body=compressed,
        ContentType="application/gzip"
    )

    print(f"Written to s3://{BUCKET}/{blob_path}")
    print(f"Records: {len(records)}")
    print(f"Size: {len(compressed) / 1024:.1f} KB")
    return blob_path

if __name__ == "__main__":
    ingest_docs()
