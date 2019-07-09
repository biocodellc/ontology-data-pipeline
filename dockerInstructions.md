# Docker Instructions
The Dockerfile in the home directory provides the necessary information for building the Docker container

# Copy necessary jar files into lib directory
We will need the following jar files for ontopilot and query_fetcher in the lib directory. Copy from ontopilot and query_fetcher directories.
```
ontopilot-2017-08-04.jar
query_fetcher-0.0.1.jar
```

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
