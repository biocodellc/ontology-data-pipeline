# Docker Instructions
The Dockerfile in the home directory provides the necessary information for building the Docker container

## build:
```
docker build -t ontology-data-pipeline .
```

## update rep:
```
docker tag ontology-data-pipeline jdeck88/ontology-data-pipeline
docker push jdeck88/ontology-data-pipeline
```
