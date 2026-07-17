# Product Docs Agent Pipeline -- AWS

End-to-end AWS pipeline that ingests product documentation from GitHub, refines it through a bronze/silver/gold medallion architecture, embeds it using Amazon Bedrock, and exposes a sub-500ms vector search tool that an AI agent can call in real time.

## Problem Statement

Product support agents fail when their knowledge is stale. Documentation lives in GitHub repositories -- updated daily by engineering teams -- but the path from "doc merged to main" to "agent can quote it" is broken in most organizations. Teams either re-index everything on a schedule (expensive, slow) or skip indexing altogether (agent answers from outdated context). Neither is acceptable when a support agent is answering thousands of queries per day.

The specific failure mode: a raw markdown file is 10,000 characters of mixed content -- headers, code blocks, installation tables, changelogs. An agent consuming that raw file spends most of its context budget on noise. Retrieval precision collapses. Answers degrade.

## What This Pipeline Does

This pipeline closes that gap by treating documentation as a first-class data engineering problem:

- Ingest raw docs from any GitHub repository on every commit, with full provenance
- Refine them through a medallion architecture -- stripping noise, normalizing structure, deduplicating by content hash
- Chunk each document into ~500-token passages with overlap, embed each chunk via Amazon Bedrock Titan Embeddings V2
- Serve results through an AWS Lambda retrieval tool that embeds an incoming query, computes cosine similarity across the full corpus, and returns ranked passages in under 500ms

The result: from "doc merged to main" to "agent can quote it" in under one hour, at near-zero marginal cost per refresh.

## Business Impact

- Eliminates full re-indexing cost: hash-gated incremental embedding means a typical hourly refresh re-embeds 0-5 chunks instead of the full corpus
- Retrieval precision over raw-file feeding: chunked passages return the exact relevant section, not a 10,000-character file the agent has to parse
- Scales to any GitHub-hosted documentation corpus without infrastructure changes
- Total operating cost under $1/month for corpora up to 10,000 chunks

## Architecture

    GitHub Repo (Markdown files)
            |
            v
    Bronze Layer (S3)
    Raw JSONL.gz partitioned by ingestion_date + commit SHA
    No transformation -- provenance baked into every record
            |
            v
    Silver Layer (S3)
    Frontmatter parsed, markdown cleaned, code fences stripped,
    deduplication by doc_path keeping latest pulled_at
            |
            v
    Gold Layer (S3)
    Each doc chunked into ~2000-char segments with 200-char overlap
    Each chunk embedded via Bedrock Titan Text Embeddings V2 (768-dim)
    Incremental: only re-embeds chunks whose content_hash changed
            |
            v
    Retrieval Tool (AWS Lambda)
    Loads gold chunks from S3 on cold start, caches in Lambda memory
    Embeds incoming query via Bedrock, computes cosine similarity,
    returns top-k ranked passages as JSON

## Current Status

| Layer | Status | Tech |
|---|---|---|
| Bronze Ingestion | Done | Python, boto3, S3 |
| Silver Refinement | Done | Python, boto3, regex, S3 |
| Gold Embeddings | Done | Amazon Bedrock Titan Embeddings V2, S3 |
| Retrieval Tool | Done | AWS Lambda, Amazon Bedrock, cosine similarity |

## S3 Bucket Layout

    s3://sarang-lake-bronze/
      source=github/
        entity=docs/
          ingestion_date=YYYY-MM-DD/
            commit=<sha8>/
              docs.jsonl.gz
        entity=docs-silver/
          ingestion_date=YYYY-MM-DD/
            silver_docs.jsonl.gz
        entity=docs-gold/
          ingestion_date=YYYY-MM-DD/
            gold_docs_chunks.jsonl.gz

## Record Schemas

Bronze:
    {
      "doc_path": "README.md",
      "content": "<raw markdown>",
      "content_hash": "<sha256>",
      "_repo": "https://github.com/aws/aws-sdk-pandas.git",
      "_commit": "<full sha>",
      "_pulled_at": "2026-07-17T18:52:00Z"
    }

Gold chunk:
    {
      "chunk_id": "<sha256[:16]>",
      "doc_path": "README.md",
      "title": "AWS SDK for pandas (awswrangler)",
      "chunk_offset": 0,
      "chunk_text": "<2000 chars of cleaned markdown>",
      "embedding": [<768 floats>],
      "embedding_model": "amazon.titan-embed-text-v2:0",
      "content_hash": "<sha256>",
      "_commit": "<full sha>",
      "_chunked_at": "2026-07-17T21:00:00Z"
    }

## Retrieval Tool

Input:
    {
      "body": "{\"query\": \"how do I install aws sdk pandas\", \"top_k\": 3}"
    }

Output:
    {
      "chunks": [
        {
          "chunk_id": "69c7a91e82a69897",
          "doc_path": "README.md",
          "title": "AWS SDK for pandas (awswrangler)",
          "chunk_text": "...",
          "score": 0.7103
        }
      ],
      "total_chunks_searched": 33,
      "embed_and_search_ms": 483,
      "query": "how do I install aws sdk pandas"
    }

Live result: top passage scored 0.71 cosine similarity, 483ms end-to-end latency.

## Design Decisions

- Idempotent S3 path: commit SHA in path means re-running at the same commit overwrites the same object, never creates duplicates
- No transformation at bronze: raw content lands verbatim -- parsing is a downstream concern
- Provenance on every record: repo URL, full commit SHA, and pulled_at timestamp on every record so the agent can attribute answers to a specific doc version
- Hash-gated incremental embedding: gold layer only re-embeds chunks whose content_hash changed -- keeps Bedrock cost near zero on hourly reruns
- In-memory vector search: 33 chunks at 768 dims fit in Lambda memory -- no vector database needed at this scale
- Cosine similarity in pure Python: no external dependencies, sub-500ms at current corpus size

## Repo Structure

    product-docs-agent-pipeline-aws/
    ingestion/
        scripts/
            ingest_docs.py
            refine_silver.py
            build_gold.py
        requirements.txt
    retrieval/
        lambda_function.py
    .gitignore
    README.md

## AWS Cost

| Resource | Cost |
|---|---|
| S3 storage (under 1MB total) | ~$0.00/month |
| Amazon Bedrock Titan Embeddings (33 chunks) | ~$0.01 one-time |
| AWS Lambda | $0.00 (free tier) |
| Total | ~$0.01 |

## Running the Pipeline

    python3 ingestion/scripts/ingest_docs.py
    python3 ingestion/scripts/refine_silver.py
    python3 ingestion/scripts/build_gold.py
