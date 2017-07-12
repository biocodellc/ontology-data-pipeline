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


## Project Config Files

Each project should have a config directory under `projects/{projectName}/config`. This will contain the files necessary
for data validation and triplifying. The following file must exist:

1. `entities.csv`

The following files are optional:

1. `rules.csv` - This file is used to setup basic validation rules for the data. The file expects the following columns:

   * `rules`
   
      The name of the validation rule to apply. See [rule types below](#rules).
      
   * `columns`
   
      Pipe `|` delimited list of columns to apply the rule to
      
   * `level`
   
      Either `WARNING` or `ERROR`. `ERROR` will terminate the program after validation. `WARNINGS` will be logged.
      Case-Insensitive. Defaults to `WARNING`
      
   * `list`
   
      Only applicable for `ControlledVocabulary` rules. This refers to the name of the list in `lists.csv` containing 
      the controlled vocab
      
   ##### <a name="rules"></a>Rule Types
   
   * `RequiredValue` - Specifies columns which can not be empty
   * `UniqueValue` - Checks that the values in a column are unique
   * `ControlledVocabulary` - Checks columns against a list of controlled vocabulary. The name of the list is specified in 
   the `list` column in `rules.csv`
   * `Integer` - Checks that all values are integers
   * `Float` - Checks that all values are floating point numbers (ex. 1.00)

2. `lists.csv` - Required if using the `ControlledVocabulary` rule. The following columns are expected:
 
   * `name`
   
     The name of the list.
     
   * `list`
   
     Pipe `|` delimited vocab list 
   
   * `case_sensitive`
   
     `true` or `false`. If this list is case sensitive or not. Defaults to `false`
