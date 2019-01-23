# A high-throughput ontology-based pipeline for data integration

A flexible, scalable pipeline for integration and alignment of multiple data sources. The code is written to be adaptable to all kinds of data, ontologies, or reasoning profiles, and output is compatible with any type of storage technology.  This codebase is designed to run in coordination with a specified ontology, written using [OWL](https://www.w3.org/OWL/). 

## Getting started

You can begin by either 1) forking this repository and installing on your laptop or server, or 2) using our [ontology-data-pipeline docker repository](https://cloud.docker.com/u/jdeck88/repository/docker/jdeck88/ontology-data-pipeline) to eliminate library and dependency headaches (see the [ovt-data-pipeline](https://github.com/futres/ovt-data-pipeline)) repository for information on how to implement docker.  The next step is familiarizing yourself with the [configuration files](https://github.com/biocodellc/ontology-data-pipeline/blob/master/README.md#config-files) and reading through the documentation on this page.  The configuration files themselves should be hosted in a different repository than this one.  Examples of two implementations of the ontology-data-pipeline are:
 
  * [ppo-data-pipeline](https://github.com/biocodellc/ppo-data-pipeline) a data pipeline for processing plant phenology observations
  * [ovt-data-pipeline](https://github.com/futres/ovt-data-pipeline) a data pipeline for processing vertebrate trait measurements

## Installation
The pipeline has been tested and run using several versions of python.  The current requirements file runs well with python 3.7.2 and so we reccomend you start with that version of python.

Visit our [python instructions](pythonInstructions.md) for installation instructions with python, if you need it.

The ontology-data-pipeline typically will need to run with another repository that contains configuration files.  Here, we offer an example of installing the ontology-data-pipeline environment in conjunction with the [ovt-data-pipeline codebase](https://github.com/biocodellc/ontology-data-pipeline).  While we use the ovt-data-pipeline codebase as an example here, you may  also create your own repository for your own sources.  If you want to create your own pipeline environment, we reccomend forking the ovt-data-pipeline or the ppo-data-pipeline configuration and starting with that.  It will make configuration much easier for your new project.

```
# create a root directory to hold code for our projects
mkdir pipeline
cd pipeline

# clone the ontology-data-pipeline
git clone https://github.com/biocodellc/ontology-data-pipeline.git

cd ontology-data-pipeline

# Install libraries (make sure you have python 3.7.2 installed)
pip install -r requirements

# verify that the tests run
python -m pytest

# If the tests pass, you can then proceed to setting up the configuration repository...
cd ..
git clone https://github.com/futres/ovt-data-pipeline.git
cd ovt-data-pipeline
# continue from there.

```

### Additional dependencies:

* Java 8
* [ontopilot](https://github.com/stuckyb/ontopilot) (Will be propted to download during cli exectuion if not found)
* [query_fetcher](https://github.com/biocodellc/query_fetcher) (Will be propted to download during cli exectuion if not found)

## Running the pipeline

Described here is the command-line tool for running the pipeline and getting started processing data.

`process.Process` is the main entry point for the application. `process.py` is a convenience wrapper script for running the
app from the source tree.

The processing pipeline implements the following steps:

1. Pre-Processing

    This step involves transforming the data into a common format for triplifying. This will usually involve writing a 
    custom `PreProcessor` for each project to be ingested. The `preprocessor` module contains an abstract class 
    `AbstractPreProcessor` that can be inherited by the project preprocessor.  We suggest adapting one of the projects included 
    in this package as a template from which to start.  The purpose of the pre-processor is to configure the data to conform to the 
    structure specified in the [headers configuration file](https://github.com/biocodellc/ppo-data-pipeline/blob/master/config/headers.csv).  
    
2. Triplifier

    This step provides provides basic data validation and generates the RDF triples, assuming validation passes, needed 
    for the reasoning phase. Each project will need to contain a `config` directory with the following files that will 
    be used to triplify the preprocessed data:
    
    NOTE: Wherever there is a uri expressed in any of the following files, you have the option of using ontology label substitution.
    If the uri is of the format `{label name here}`, the appropriate uri will be substituted from the provided ontology. See the [OntoPilot Documentation](https://github.com/stuckyb/ontopilot/wiki/Ontology-development#column-names-for-all-ontology-entities) for details term identifier abbreviations.
    
    1. [entity.csv](#entity.csv)
    2. [mapping.csv](#mapping.csv)
    3. [relations.csv](#relations.csv)
    4. [phenophase_descriptions.csv](#descriptions)
    5. [rules.csv](#rules.csv)
    
3. Reasoning

    This step uses the [ontopilot](https://github.com/stuckyb/ontopilot) project to perform inferencing using the
    [Plant Phenology Ontology](https://github.com/PlantPhenoOntology/ppo)
    
4. Rdf2Csv

    This step takes the provided [sparql query](#fetch_reasoned.sparql) and generates csv files for each file outputted
    by in the Reasoning step. If no sparql query is found, then this step is skipped.
    
5. Data Loading

    This is a separate cli used for loading reasoned data into elasticsearch and/or blazegraph.
    
    `loader.loader` is the main entry point for the application. `loader.py` is a convenience wrapper script for running the
    app from the source tree.

    * Uploading
    
        1. BlazeGraph
        2. ElasticSearch
        
   



## Usage

Before running the processing script, you will likely need to fetch data from your source projects.
We reccomend putting this data into a data subdirectory. If the data is large you may want to ignore the data
directory in your .gitignore file.

Running from the process.py script:

```
$ python process.py --help

usage: process.py [-h] [--data_file DATA_FILE] [--preprocessor PREPROCESSOR]
                  [--drop_invalid] [--log_file]
                  [--reasoner_config REASONER_CONFIG] [-v] [-c CHUNK_SIZE]
                  [--num_processes NUM_PROCESSES] [-s SPLIT_DATA_COLUMN]
                  project input_dir output_dir ontology config_dir
                  project_base

PPO data pipeline cmd line application.

positional arguments:
  project               This is the name of the directory containing the
                        project specific files. All project config
                        directoriesmust be placed in the `projects` directory.
  input_dir             path of the directory containing the data to process
  output_dir            path of the directory to place the processed data
  ontology              specify a filepath/url of the ontology to use for
                        reasoning/triplifying
  config_dir            Specify the path of the directory containing the
                        configuration files.
  project_base          Specify where the python modules reside for the
                        preprocessor live. This is specified in python dotted
                        notation. The base_dir (base directory) is set using
                        the project_base with a directory name for the project

optional arguments:
  -h, --help            show this help message and exit
  --data_file DATA_FILE
                        optionally specify the data file to load. This will
                        skip the preprocessor step and used the supplied data
                        file instead
  --preprocessor PREPROCESSOR
                        optionally specify the dotted python path of the
                        preprocessor class. This will be loaded instead of
                        looking for a PreProcessor in the supplied project
                        directory. Ex: projects.asu.proprocessor.PreProcessor
  --drop_invalid        Drop any data that does not pass validation, log the
                        results, and continue the process
  --log_file            log all output to a log.txt file in the output_dir.
                        default is to log output to the console
  --reasoner_config REASONER_CONFIG
                        optionally specify the reasoner configuration file
  -v, --verbose         verbose logging output
  -c CHUNK_SIZE, --chunk_size CHUNK_SIZE
                        chunk size to use when processing data. optimal
                        chunk_size for datasets with less then 200000
                        recordscan be determined with: num_records / num_cpus
  --num_processes NUM_PROCESSES
                        number of process to use for parallel processing of
                        data. Defaults to cpu_count of the machine
  -s SPLIT_DATA_COLUMN, --split_data SPLIT_DATA_COLUMN
                        column to split the data on. This will split the data
                        file into many files with each file containing no more
                        records then the specified chunk_size, using the
                        specified column values as the filenames

As an alternative to the commandline, params can be placed in a file, one per
line, and specified on the commandline like 'process.py @params.conf'.


```

Example of running the processing script which will run the pre-processor and all 
dependencies, specifying a local copy of the PPO ontology using nohup and running background:
```
nohup python process.py --drop_invalid --num_processes 4 \
    npn data/npn/input/ data/npn/output/ \
    file:/home/jdeck/code/ppo-data-pipeline/config/ppo.owl \
    config/ projects/ &
```

Running the loader.py script:

```
16:38 $ python loader.py --help
usage: loader.py [-h] [-rdf_i--rdf_input_dir RDF_I__RDF_INPUT_DIR]
                 [--endpoint ENDPOINT] [-es_ies_input_dir ES_IES_INPUT_DIR]
                 [--index INDEX] [--drop-existing] [--alias ALIAS]
                 {both,blazegraph,elasticsearch}

data loading cmd line application for PPO data pipeline.

positional arguments:
  {both,blazegraph,elasticsearch}

optional arguments:
  -h, --help            show this help message and exit

blazegraph:
  blazegraph loading options

  -rdf_i--rdf_input_dir RDF_I__RDF_INPUT_DIR
                        The path of the directory containing the rdf data to
                        upload to blazegraph
  --endpoint ENDPOINT   the blazegraph endpoint to upload to. The namespace
                        will be the name of the uploadedfile minus the
                        extension

elastic_search:
  elastic_search loading options

  -es_ies_input_dir ES_IES_INPUT_DIR
                        The path of the directory containing the csv data to
                        upload to elasticsearch
  --index INDEX         The name elasticsearch of the index to upload to
  --drop-existing       this flag will drop all existing data with the same
                        "source" value.
  --alias ALIAS         optionally specify an elastic search alias. When
                        creating an index, it will be associatedwith this
                        alias

As an alternative to the commandline, params can be placed in a file, one per
line, and specified on the commandline like 'loader.py @params.conf'.
```
An example of running the loading script (ensure proper IP access to tarly.cyverse.org):
```
python loader.py --es_input_dir data/npn/output/output_reasoned_csv/ --index npn --drop-existing --alias ppo --host tarly.cyverse.org:80 elasticsearch
python loader.py --es_input_dir data/neon/output/output_reasoned_csv/ --index neon --drop-existing --alias ppo --host tarly.cyverse.org:80 elasticsearch
python loader.py --es_input_dir data/pep725/output/output_reasoned_csv/ --index pep725 --drop-existing --alias ppo --host tarly.cyverse.org:80 elasticsearch
```

## Config Files

Project configuration files include [`entity.csv`](#entity.csv), [`mapping.csv`](#mapping.csv), [`relations.csv`](#relations.csv), and any files defining controlled vocabularies that we want to map rdf:types to.  The remaining configuration files below are found in the `config` directory.  Together, these are the required configuration files we use for reasoning against the application ontology (e.g. [Plant Phenology Ontology](https://github.com/PlantPhenoOntology/PPO/)). These files configure the data validation, triplifying, reasoning, and rdf2csv converting.
 
The following files are required:

##### <a name="entity.csv"></a>

1. `entity.csv` (found in each project directory) - This file specifies the entities to create when triplifying. The file expects the following columns:

    * `alias`
        
        The name used to refer to the entity
        
    * `concept_uri`
    
        The uri which defines this entity
        
    * `unique_key`
    
        The column used to uniquely identify the entity
        
    * `identifier_root`
    
        The identifier root for each unique entity. This is typically an [BCID](http://biscicol.org) identifier
    
##### <a name="mapping.csv"></a>
2. `mapping.csv` (found in each project directory)

    * `column`
    
        The name of the column in the csv file to be used for triplifying
        
    * `uri`
    
        The uri which defines this column
        
    * `entity_alias`
    
        The alias of the entity this column is a property of
        
##### <a name="relations.csv"></a>
3. `relations.csv` (found in each project directory)

    * `subject_entity_alias`
    
        The alias of the entity which is the subject of this relationship
        
    * `predicate`
    
        The uri which defines the relationship
        
    * `object_entity_alias`
    
        The alias of the entity which is the object of this relationship       
        
4. `excluded_types.csv` - Used by ontopilot

5. `reasoner.conf` - ontopilot inferencing configuration file

6. `headers.csv` - specifies the input data headers we except to see after preprocessing the data
    

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
   
