# A high-throughput ontology-based pipeline for data integration

A flexible, scalable pipeline for integration of observation data from multiple sources. Little or no programming is required to adapt the pipeline for use with different kinds of data, ontologies, or reasoning profiles, and output is compatible with any type of storage technology. 

## Getting started

You can begin by cloning or forking this repository and installing on your laptop or server.  The next step is familiarizing yourself with the [configuration files](https://github.com/biocodellc/ppo-data-pipeline/blob/master/README.md#config-files) and reading through the documentation on this page.  

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
        
    
## Dependencies


### Install python
Reccomended running python version 3.5.1 , installed using [pyenv](https://github.com/pyenv/pyenv)

The python dependencies are found in `requirements.txt`. These can be installed by running 

```pip install -r requirements.txt --user```


### Installing Python in Mac Environment
```
brew install pyenv
```

Also reccomend using virtual environments, for example:
```
pyenv install 3.5.1

brew install pyenv-virtaulenv
pyenv virtualenv 3.5.1 ppo-pipeline

# this will automatically activate this environment
# in the directory here
pyenv local ppo-pipeline
```
### Additional dependencies:

* Java 8
* [ontopilot](https://github.com/stuckyb/ontopilot) (Will be propted to download during cli exectuion if not found)
* [query_fetcher](https://github.com/biocodellc/query_fetcher) (Will be propted to download during cli exectuion if not found)


## Usage

Before running the processing script, you will likely need to fetch data.  Some of the
projects likely will have an API that you can obtain data from and these are written 
into a data_fetcher.py script for NPN and NEON. PEP725 data needs mysql tables to be
extracted manually:

```
python ./projects/npn/data_fetcher.py data/npn/input/
python ./projects/neon/data_fetcher.py data/neon/input/
```

NOTE: when updating data, we currently need to manually update the citation and data 
usage policy file with the date of load, which is found at the following location (after
updating this file we need to git pull changes in the ppo-data-server repository):
```
https://raw.githubusercontent.com/biocodellc/ppo-data-server/master/citation_and_data_use_policies.txt
```

Running from the process.py script:

```
$ python process.py --help
usage: process.py [-h] (--input_dir INPUT_DIR | --data_file DATA_FILE)
                  [--config_dir CONFIG_DIR] [--ontology ONTOLOGY]
                  [--preprocessor PREPROCESSOR] [--drop_invalid] [--log_file]
                  [--reasoner_config REASONER_CONFIG] [-v] [-c CHUNK_SIZE]
                  [--num_processes NUM_PROCESSES] [-s SPLIT_DATA_COLUMN]
                  project output_dir

PPO data pipeline cmd line application.

positional arguments:
  project               This is the name of the directory containing the
                        project specific files. All project config
                        directoriesmust be placed in the `projects` directory.
  output_dir            path of the directory to place the processed data

optional arguments:
  -h, --help            show this help message and exit
  --input_dir INPUT_DIR
                        path of the directory containing the data to process
  --data_file DATA_FILE
                        optionally specify the data file to load. This will
                        skip the preprocessor step and used the supplied data
                        file instead
  --config_dir CONFIG_DIR
                        optionally specify the path of the directory
                        containing the configuration files. defaults to
                        /Users/rjewing/code/biocode/ppo-data-
                        pipeline/process/../config
  --ontology ONTOLOGY   optionally specify a filepath/url of the ontology to
                        use for reasoning/triplifying
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
Examples of loading the processing script which will run the pre-processor and all 
dependencies, specifying a local copy of the PPO ontology using nohup and running background:
```
nohup python process.py --ontology file:/home/jdeck/code/ppo-data-pipeline/config/ppo.owl \
    --input_dir data/npn/input/ --drop_invalid npn data/npn/output/ --num_processes 4 &
nohup python process.py --ontology file:/home/jdeck/code/ppo-data-pipeline/config/ppo.owl \
    --input_dir data/neon/input/ --drop_invalid neon data/neon/output/ --num_processes 4 &
nohup python process.py --ontology file:/home/jdeck/code/ppo-data-pipeline/config/ppo.owl \
    --input_dir data/pep725/input/ --drop_invalid pep725 data/pep725/output/ --num_processes 4 &
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

We provide a set of default configuration files found under `config` directory as well as in each project directory.   Project configuration files include [`entity.csv`](#entity.csv), [`mapping.csv`](#mapping.csv), [`relations.csv`](#relations.csv), and [`phenophase_descriptions.csv`](https://github.com/biocodellc/ppo-data-pipeline#descriptions).  The remaining configuration files below are found in the `config` directory.  Together, these are the required configuration files we use for reasoning against the application ontology (e.g. [Plant Phenology Ontology](https://github.com/PlantPhenoOntology/PPO/)). These files configure the data validation, triplifying, reasoning, and rdf2csv converting.
 
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
        
##### <a name="descriptions"></a>
4. <a name="pheno_descriptions"></a>`phenophase_descriptions.csv` (found in each project directory)

    * `field`
    
        The name of the field in the input csv file
        
    * `defined_by`
    
        The uri which defines the field
        
5. `excluded_types.csv` - Used by ontopilot

6. `reasoner.conf` - ontopilot inferencing configuration file

7. `headers.csv` - specifies the input data headers we except to see after preprocessing the data
    

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
   
