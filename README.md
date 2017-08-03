# Plant Phenology Ontology Data Processor

python cli processing pipeline for processing ontology data.

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
    
    NOTE: Wherever there is a uri expressed in any of the following files, you have the option of using ontology label substitution
    If the uri is of the format `{label name here}`, the appropriate uri will be substituted from the provided ontology
    
    1. [entity.csv](#entity.csv)
    2. [mapping.csv](#mapping.csv)
    3. [relations.csv](#relations.csv)
    4. [phenophase_descriptions.csv](#descriptions)
    5. [rules.csv](#rules.csv)
    
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
```
$ python process.py -h
usage: process.py [-h] (--input_dir INPUT_DIR | --data_file DATA_FILE)
                  [--preprocessor PREPROCESSOR] [--drop_invalid] [--log_file]
                  [-v]
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
  --preprocessor PREPROCESSOR
                        optionally specify the dotted python path of the
                        preprocessor class. This will be loaded instead of
                        looking for a PreProcessor in the supplied project
                        directory. Ex: projects.asu.proprocessor.PreProcessor
  --drop_invalid        Drop any data that does not pass validation, log the
                        results, and continue the process
  --log_file            log all output to a log.txt file in the output_dir.
                        default is to log output to the console
  -v, --verbose         verbose logging output

As an alternative to the commandline, params can be placed in a file, one per
line, and specified on the commandline like 'process.py @params.conf'.
```


## Config Files

Each project should have a config directory under `projects/{projectName}/config`. This will contain the files necessary
for data validation and triplifying. The following files are required:

##### <a name="entity.csv"></a>

1. `entity.csv` - This file specifies the entities to create when triplifying. The file expects the following columns:

    * `alias`
        
        The name used to refer to the entity
        
    * `concept_uri`
    
        The uri which defines this entity
        
    * `unique_key`
    
        The column used to uniquely identify the entity
        
    * `identifier_root`
    
        The identifier root for each unique entity. This is typically an [BCID](http://biscicol.org) identifier
    
##### <a name="mapping.csv"></a>
2. `mapping.csv`

    * `column`
    
        The name of the column in the csv file to be used for triplifying
        
    * `uri`
    
        The uri which defines this column
        
    * `entity_alias`
    
        The alias of the entity this column is a property of
        
##### <a name="relations.csv"></a>
3. `relations.csv`

    * `subject_entity_alias`
    
        The alias of the entity which is the subject of this relationship
    * `predicate`
    
        The uri which defines the relationship
        
    * `object_entity_alias`
    
        The alias of the entity which is the object of this relationship
        
##### <a name="descriptions"></a>
4. <a name="pheno_descriptions"></a>`phenophase_descriptions.csv`

    * `field`
    
        The name of the field in the input csv file
        
    * `defined_by`
    
        The uri which defines the field
    

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
