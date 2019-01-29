# Docker Instructions
The Dockerfile in the home directory provides the necessary information for building the Docker container

## build:
```
docker build -t ontology-data-pipeline .
```

## update rep:
```
# if necessary:
docker login --username=jdeck88
# not sure if we have to do this everytime:
docker tag ontology-data-pipeline jdeck88/ontology-data-pipeline
# push image to docker hub
docker push jdeck88/ontology-data-pipeline

# From the client we need to pull occasionally.. 
docker pull jdeck88/ontology-data-pipeline
```
