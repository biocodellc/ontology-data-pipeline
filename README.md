# A high-throughput ontology-based pipeline for data integration

A flexible, scalable pipeline for integration and alignment of multiple data sources. The code is written to be adaptable to all kinds of data, ontologies ([OWL](https://www.w3.org/OWL/)), or reasoning profiles, and output is compatible with any type of storage technology. 

## Applications developed to use ontology-data-pipeline
A good way to start with the ontology-data-pipeline is to fork or clone one of the applications which use this code.  This includes:

  * [fovt-data-pipeline](https://github.com/futres/fovt-data-pipeline) a data pipeline for processing vertebrate trait measurements
  * [ppo-data-pipeline](https://github.com/biocodellc/ppo-data-pipeline) a data pipeline for processing plant phenology observations
  

## Quick Start

Step 1: [Install docker](https://docs.docker.com/install/)

Step 2: Run the application. On the commandline, you can execute the script like so:
```
# make sure you have the latest docker container
docker pull jdeck88/ontology-data-pipeline
# run the pipeline help in the docker container
docker run -v "$(pwd)":/process -w=/app -ti jdeck88/ontology-data-pipeline python pipeline.py -h 
```


## Configuring Your Environment
![Ontology Data Pipeline Diagram](https://raw.githubusercontent.com/biocodellc/ontology-data-pipeline/master/pipelineImage.png)

The ontology-data-pipeline operates on a set of configuration files, which you can specify 
in the configuration directory.

The following text describes the operation of the pipeline and the steps involved.

1. Triplifier

    This step provides provides basic data validation and generates the RDF triples, assuming validation passes, needed 
    for the reasoning phase. Each project will need to contain a `config` directory with the following files that will 
    be used to triplify the preprocessed data:
    
    NOTE: Wherever there is a uri expressed in any of the following files, you have the option of using ontology label substitution.
    If the uri is of the format `{label name here}`, the appropriate uri will be substituted from the provided ontology. See the [ROBOT](http://robot.obolibrary.org/) for details term identifier abbreviations.
    
    1. [entity.csv](#entity.csv)
    2. [mapping.csv](#mapping.csv)
    3. [relations.csv](#relations.csv)
    4. [rules.csv](#rules.csv)
    
2. Reasoning

    This step uses the [ROBOT](http://robot.obolibrary.org/) project to perform reasoning on the triplified data in the triplifier step, in conjunction with logic contained in the provided ontology. An example of the use of the ROBOT file is given below, calling the robot.jar which is in the `/lib` directory:

```
 java -jar lib/robot.jar reason -r elk \
    --axiom-generators "InverseObjectProperties ClassAssertion" \
    -i sample_data/unreasoned_data.ttl \
    --include-indirect true \
    --exclude-tautologies structural \
    reduce \
    -o sample_data/reasoned_data.ttl
```
In the above example, the reasoner use is ELK, with InverseObjectProperties and ClassAssertion axioms specified.  The input file is `sample_data/unreasoned_data.ttl`.  We tell the reasoning engine to include indirect inferences, which lets us assert recursive SubClass relationships.  Exclude tautoligies tells the reasoner to not include assertions which will always be true.  The reduce command eliminates redundant assertions.  Finally, the output file is `sample_data/reasoned_data.ttl`.   The most critical step of the reasoning process is including the indirect inferences: this is the step that lets us export our end-results to a simple Document Store and not rely on "smart" applications which are able to iterate recursive relationships. Examples included in the test directory.
    
3. Data Formatting

    This step takes the provided [sparql query](#sample_data/fetch_reasoned.sparql) and generates csv files for each file outputted
    in the Reasoning step using [ROBOT](http://robot.obolibrary.org/). If no sparql query is found, then this step is skipped.
An example of the data Formatting step is given below calling the robot.jar which is in the `/lib` directory:

```
java -jar lib/robot.jar query --input sample_data/reasoned_data.ttl \
    --query sample_data/fetch_reasoned.sparql \
    sample_data/reasoned_data.ttl.csv
```

In the above example, we call the ROBOT query sub-command and give an input file of `sample_data/reasoned_data.ttl` (this is the output of the above command), and tell it to use the `sample_data/fetch_reasoned.sparql` sparql command as a guide to produce the output file `sample_data/reasoned_data.ttl.csv`.  Examples included in the test directory.

    

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
    
        The column name that is used to uniquely identify the entity. Whenever there is a unique value for the property specified by "unique key", a new instance will be created. e.g. "record_id"
        
    * `identifier_root`
    
        The identifier root for each unique entity (instance created). E.g. urn:observingprocess/ would be the root of urn:observingprocess/record1
    
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

The ontology-data-pipeline is designed to be run as a Docker container.  However, you can also run the codebase from sources by checking out this repository and following the instructions at [python instructions](pythonInstructions.md).
Information on building the docker container is contained at [docker instructions](dockerInstructions.md).
