# Python Instructions

These instructions assume you want to use the ontology-data-pipeline as a basis for developing your own pipeline environment.  Developers wishing to commit to this codebase are free to submit pull requests.

## Installation
The pipeline has been tested and run using several versions of python.  The current requirements file runs well with python 3.6.8 and so we reccomend you start with that version of python.

Clone the [ontology-data-pipeline](https://github.com/biocodellc/ontology-data-pipeline) repository and then verify installation:
```
# Install libraries (make sure you have python 3.6.8 installed)
pip install -r requirements

# verify that the tests run
python -m pytest
```

Next, we reccomend you fork the [fovt-data-pipeline](https://github.com/futres/fovt-data-pipeline) and place at the same level in the directory tree as ontology-data-pipeline.
  
You will also need to clone the [ontopilot](https://github.com/stuckyb/ontopilot) repository and place at the same level in the directory tree and then create a symbolic link in root directory of your pipeline environment `ln -s ../ontopilot ontopilot` (this is required for referencing in the reasoner step)

When done, you should have the following directories:
```
\code\root\ontology-data-pipeline
\code\root\my-customized-data-pipeline
\code\root\ontopilot
```

Once you have completed the above steps, you can run the following within the `my-customized-data-pipeline` code, substituting your input data file (replacing `sample_data_processed.csv`) when you are ready:

```
# a sample_runner script running python directly assuming that you have ontology-data-pipeline checked out
# in the proper location
    python ../ontology-data-pipeline/process.py \
    -v --drop_invalid \
    sample_data_processed.csv \
    data/output \
    https://raw.githubusercontent.com/futres/fovt/master/fovt.owl \
    config \
```


