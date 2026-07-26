.PHONY: install test lint ingest refine embed pipeline deploy-lambda

install:
	pip install -r requirements.txt -r requirements-dev.txt

test:
	pytest tests/ -v --cov=pipeline --cov=retrieval --cov-report=term-missing --cov-fail-under=70

lint:
	ruff check .

ingest:
	python -m pipeline.bronze.ingest

refine:
	python -m pipeline.silver.refine

embed:
	python -m pipeline.gold.embed

pipeline: ingest refine embed

# Packages pipeline/ + retrieval/ with their runtime dependencies and deploys
# to the existing search-docs Lambda function. Requires AWS credentials with
# lambda:UpdateFunctionCode on search-docs, and the aws CLI on PATH.
deploy-lambda:
	rm -rf build/lambda_package retrieval_lambda.zip
	mkdir -p build/lambda_package
	pip install -r requirements.txt -t build/lambda_package --quiet
	cp -r pipeline retrieval build/lambda_package/
	cd build/lambda_package && zip -r ../../retrieval_lambda.zip . -x '*.pyc' -x '__pycache__/*'
	aws lambda update-function-code \
		--function-name search-docs \
		--zip-file fileb://retrieval_lambda.zip \
		--region $${AWS_REGION:-us-east-1}
