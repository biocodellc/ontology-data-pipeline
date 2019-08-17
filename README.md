# A high-throughput ontology-based pipeline for data integration

A flexible, scalable pipeline for integration and alignment of multiple data sources. The code is written to be adaptable to all kinds of data, ontologies ([OWL](https://www.w3.org/OWL/)), or reasoning profiles, and output is compatible with any type of storage technology. 

## Applications developed to use ontology-data-pipeline
A good way to start with the ontology-data-pipeline is to look at applications which use this code.  This includes:

  * [ppo-data-pipeline](https://github.com/biocodellc/ppo-data-pipeline) a data pipeline for processing plant phenology observations
  * [fovt-data-pipeline](https://github.com/futres/fovt-data-pipeline) a data pipeline for processing vertebrate trait measurements


## Quick Start

Step 1: [Install docker](https://docs.docker.com/install/)

Step 2: Run the application. On the commandline, you can execute the pipeline, like:
```
# make sure you have the latest docker container
docker pull jdeck88/ontology-data-pipeline
# run the pipeline help in the docker container
docker run -v "$(pwd)":/process -w=/app -ti jdeck88/ontology-data-pipeline python pipeline.py -h 
```

An example script for running the application ...
```
# check that we have the latest ...
docker pull jdeck88/ontology-data-pipeline

docker run -v "$(pwd)":/process -w=/app -ti jdeck88/ontology-data-pipeline \
    python pipeline.py \
    -v --drop_invalid \
    /process/data/data.csv \
    /process/data/processed \
    https://raw.githubusercontent.com/futres/fovt/master/ontology/fovt-merged-reasoned.owl \
    /process/config \
```

## Configuring Your Environment
The ontology-data-pipeline operates on a set of configuration files, which you can specify 
in the configuration directory.

The following text describes the operation of the pipeline and the steps involved.

1. Triplifier

    This step provides provides basic data validation and generates the RDF triples, assuming validation passes, needed 
    for the reasoning phase. Each project will need to contain a `config` directory with the following files that will 
    be used to triplify the preprocessed data:
    
    NOTE: Wherever there is a uri expressed in any of the following files, you have the option of using ontology label substitution.
    If the uri is of the format `{label name here}`, the appropriate uri will be substituted from the provided ontology. See the [OntoPilot Documentation](https://github.com/stuckyb/ontopilot/wiki/Ontology-development#column-names-for-all-ontology-entities) for details term identifier abbreviations.
    
    1. [entity.csv](#entity.csv)
    2. [mapping.csv](#mapping.csv)
    3. [relations.csv](#relations.csv)
    4. [rules.csv](#rules.csv)
    
2. Reasoning

    This step uses the [ontopilot](https://github.com/stuckyb/ontopilot) project to perform reasoning on the triplified data in the triplifier step, in conjunction with logic contained in the provided ontology. 
    
3. Rdf2Csv

    This step takes the provided [sparql query](#fetch_reasoned.sparql) and generates csv files for each file outputted
    in the Reasoning step. If no sparql query is found, then this step is skipped.
    
4. Data Loading

    This is a separate cli used for loading reasoned data into elasticsearch and/or blazegraph.
    
    `loader.loader` is the main entry point for the application. `loader.py` is a convenience wrapper script for running the
    app from the source tree.

    * Uploading
    
        1. BlazeGraph
        2. ElasticSearch
        
Getting help with the loader.py script:
```
docker run -t -v "$(pwd)":/process -w=/app -ti jdeck88/ontology-data-pipeline python loader.py -h
```
As an alternative to the commandline, params can be placed in a file, one per
line, and specified on the commandline like 'loader.py @params.conf'.

An example of running the loading script (ensure proper IP access to tarly.cyverse.org):
```
python loader.py --es_input_dir data/npn/output/output_reasoned_csv/ --index npn --drop-existing --alias ppo --host tarly.cyverse.org:80 elasticsearch
```

## Config Files

Project configuration files include [`entity.csv`](#entity.csv), [`mapping.csv`](#mapping.csv), [`relations.csv`](#relations.csv), and any files defining controlled vocabularies that we want to map rdf:types to.  The remaining configuration files below are found in the `config` directory.  Together, these are the required configuration files we use for reasoning against the application ontology (e.g. [Plant Phenology Ontology](https://github.com/PlantPhenoOntology/PPO/)). These files configure the data validation, triplifying, reasoning, and rdf2csv converting.
 
The following files are required:

##### <a name="entity.csv"></a>

1. `entity.csv` (found in each project directory) - This file specifies the entities (instances of classes) to create when triplifying. The file expects the following columns:

    * `alias`
        
        The name used to refer to the entity. This is usually a shortened version of the class label.
        
    * `concept_uri`
    
        The uri which defines this entity (class).
        
    * `unique_key`
    
        The column used to uniquely identify the entity. Whenever there is a unique value for the property specified by "unique key", a new instance will be created.
        
    * `identifier_root`
    
        The identifier root for each unique entity (instance created). This is typically an [BCID](http://biscicol.org) identifier
    
##### <a name="mapping.csv"></a>
2. `mapping.csv` (found in each project directory)

    * `column`
    
        The name of the column in the csv file to be used for triplifying
        
    * `uri`
    
        The uri which defines this column. These generally are data properties.
        
    * `entity_alias`
    
        The alias of the entity (from entity.csv) this column is a property of
        
##### <a name="relations.csv"></a>
3. `relations.csv` (found in each project directory)

    * `subject_entity_alias`
    
        The alias of the entity which is the subject of this relationship
        
    * `predicate`
    
        The uri which defines the relationship
        
    * `object_entity_alias`
    
        The alias of the entity which is the object of this relationship 
        
 The terms in this file come from the source ontology.
        
4. `excluded_types.csv` - Used by ontopilot to specify the ontology classes for which instances will NOT be created during reasoning. You can choose to exlude a class or its ancestors or both. This prevents the creation of unneeded instances for root level classes on which no one is likely to query.

5. `reasoner.conf` - ontopilot inferencing configuration file


The following files are optional:

##### <a name="rules.csv"></a>

1. `rules.csv` - This file is used to setup basic validation rules for the data. The file expects the following columns:

   * `rule`
   
      The name of the validation rule to apply. See [rule types below](#rules). Note: a default `ControlledVocabulary`
      rule will be applied to the `phenophase_name` column for the names found in the [phenophase_descriptions.csv](#pheno_descriptions) 
      file
      
   * `columns`
   
      Pipe `|` delimited list of columns to apply the rule to
      
   * `level`
   
      Either `WARNING` or `ERROR`. `ERROR` will terminate the program after validation. `WARNINGS` will be logged.
      Case-Insensitive. Defaults to `WARNING`
      
   * `list`
   
      Only applicable for `ControlledVocabulary` rules. This refers to the name of the file that contains the list of
      the controlled vocab
      
   ##### <a name="rules"></a>Rule Types
   
   * `RequiredValue` - Specifies columns which can not be empty
   * `UniqueValue` - Checks that the values in a column are unique
   * `ControlledVocabulary` - Checks columns against a list of controlled vocabulary. The name of the list is specified in 
   the `list` column in `rules.csv`
   * `Integer` - Checks that all values are integers. Will coerce values to integers if possible
   * `Float` - Checks that all values are floating point numbers (ex. 1.00). Will coerce values to floats if possible

2. Any file specified in `rules.csv` `list` column is required. The file expects the following columns:

    * `field` - Specifies a valid value. This is the values expected in the input data file
    * `defined_by` - Optional value which will replace the field when writing triples
    
3. <a name="fetch_reasoned.sparql"></a>`fetch_reasoned.sparql` - Sparql query used to convert reasoned data to csv
   
## Developers

The ontology-data-pipeline is designed to be run as a Docker container.  However, you can also run the codebase from sources by 
checking out this repository and following the instructions at [python instructions](pythonInstructions.md).
Information on building the docker container is contained at [docker instructions](dockerInstructions.md).
