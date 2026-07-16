# Product Docs Agent Pipeline -- AWS

End-to-end pipeline that ingests product documentation from GitHub, refines it through a bronze/silver/gold medallion architecture on AWS, and serves it to an AI agent via a retrieval tool.

## Current Status

| Layer | Status | Tech |
|---|---|---|
| Bronze Ingestion | Done | Python, boto3, S3 |
| Silver Refinement | In progress | AWS Glue / PySpark |
| Gold Embeddings | Planned | Amazon Bedrock |
| Retrieval Tool | Planned | AWS Lambda |

## Design Decisions

- Idempotent path key: commit SHA in path prevents duplicates on reruns
- No transformation at bronze: raw content lands verbatim
- Provenance on every record: repo, commit, timestamp baked in
- Single gzipped JSONL per run: all docs under 10MB

## Cost Warning

Current cost: $0.00. S3 storage under 1MB. Will update as layers are added.
