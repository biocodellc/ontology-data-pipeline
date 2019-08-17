# Docker Instructions
The Dockerfile in the home directory provides the necessary information for building the Docker container

## build:
Docker is built automatically at docker hub when a commit is pushed to master

## Manual Method uses 
This section only necessary if the container is not automatically built.
```
docker build -t ontology-data-pipeline

# if necessary:
docker login --username=jdeck88
# not sure if we have to do this everytime:
docker tag ontology-data-pipeline jdeck88/ontology-data-pipeline
# push image to docker hub
docker push jdeck88/ontology-data-pipeline

# From the client we need to pull occasionally.. 
docker pull jdeck88/ontology-data-pipeline
```
