# A tiny agent image for the capstone pipeline: Python + the Kafka client.
# Build once and push to your local registry, then the pipeline runs on it
# (no pip install per build):
#   docker build -t localhost:5001/kafka-python:1 .
#   docker push  localhost:5001/kafka-python:1
FROM python:3.12-slim
RUN pip install --no-cache-dir kafka-python-ng==2.2.3
