# Product Docs Agent Pipeline -- AWS

End-to-end pipeline that ingests product documentation from GitHub, refines it through a bronze/silver/gold medallion architecture on AWS, and serves it to an AI agent via a retrieval tool.

## Current Status

| Layer | Status | Tech |
|---|---|---|
| Bronze Ingestion  | Done | Python, boto3, S3 |
| Silver Refinement | Done | Python, boto3, S3 |
| Gold Embeddings   | Done | Amazon Bedrock Titan Embeddings V2 |
| Retrieval Tool    | In progress | AWS Lambda |

## Design Decisions

- Idempotent path key: commit SHA in path prevents duplicates on reruns
- No transformation at bronze: raw content lands verbatim
- Provenance on every record: repo, commit, timestamp baked in
- Single gzipped JSONL per run: all docs under 10MB

## Cost Warning

Current cost: ~$0.01 (Amazon Bedrock Titan Embeddings — 33 chunks embedded)
