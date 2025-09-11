#!/bin/bash

# 開発環境用のDockerイメージをビルドしてArtifact Registryにプッシュするスクリプト

PROJECT_ID="marketing-automation-461305"
REGION="asia-northeast1"
REPOSITORY="cloud-run-source-deploy"
COMMIT_SHA=$(git rev-parse HEAD)

echo "Building and pushing development images with commit SHA: ${COMMIT_SHA}"

# 1. Main service image (cohere-rag-dev)
echo "Building cohere-rag-dev image..."
docker build -t ${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/cohere-rag-dev:${COMMIT_SHA} \
  -t ${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/cohere-rag-dev:latest \
  -f Dockerfile .

echo "Pushing cohere-rag-dev image..."
docker push ${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/cohere-rag-dev:${COMMIT_SHA}
docker push ${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/cohere-rag-dev:latest

# 2. Vectorize Job image (cohere-rag-vectorize-job-dev)
echo "Building cohere-rag-vectorize-job-dev image..."
docker build -t ${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/cohere-rag-vectorize-job-dev:${COMMIT_SHA} \
  -t ${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/cohere-rag-vectorize-job-dev:latest \
  -f docker/Dockerfile.job.dev .

echo "Pushing cohere-rag-vectorize-job-dev image..."
docker push ${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/cohere-rag-vectorize-job-dev:${COMMIT_SHA}
docker push ${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/cohere-rag-vectorize-job-dev:latest

# 3. Scheduler Job image (scheduler-job-dev)
echo "Building scheduler-job-dev image..."
docker build -t ${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/scheduler-job-dev:${COMMIT_SHA} \
  -t ${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/scheduler-job-dev:latest \
  -f docker/Dockerfile.scheduler.dev .

echo "Pushing scheduler-job-dev image..."
docker push ${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/scheduler-job-dev:${COMMIT_SHA}
docker push ${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/scheduler-job-dev:latest

echo "All development images have been built and pushed successfully!"
echo ""
echo "Images created:"
echo "- ${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/cohere-rag-dev:${COMMIT_SHA}"
echo "- ${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/cohere-rag-vectorize-job-dev:${COMMIT_SHA}"
echo "- ${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/scheduler-job-dev:${COMMIT_SHA}"