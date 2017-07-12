# Plant Phenology Ontology Data Processor

This repo is a python cli processing pipeline.

`process.Process` is the main entry point for the application. `process.py` is a convince wrapper script for running the
app from the source tree.

The processing pipeline implements the following steps:

1. Pre-Processing

    This step involves transforming the data into a common format for triplifying. This will usually involve writing a 
    custom `PreProcessor` for each project to be ingested. The `preprocessor` module contains an abstract class 
    `AbstractPreProcessor` that is meant to be inherited by the project preprocessor. That class contains information about the 
    expected input format for the triplifying step
    
2. Triplifyer

    This step provides provides basic data validation and generates the RDF triples, assuming validation passes, needed 
    for the reasoning phase. Each project will need to contain a `config` directory with the following files that will 
    be used to triplify the preprocessed data:
    
    TODO finish the following and give more information about each file
    1. entities.csv
    2. rules.csv
    
3. Reasoning

    This step uses the [ontopilot](https://github.com/stuckyb/ontopilot) project to perform inferencing using the
    [Plant Phenology Ontology](https://github.com/PlantPhenoOntology/ppo)

4. Uploading

    TODO flesh this out
    1. BlazeGraph
    2. ElasticSearch
    
## Usage

Running from the process.py script:

TODO update with help output
```shell
usage: process.py [-h] [--preprocessor PREPROCESSOR]
                  project input_dir output_dir
```