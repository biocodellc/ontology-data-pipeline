# Data Driven Test example directory

This directory serves as an end to end testing location for project datasets.
The environment here replicates the project root directory that invokes the 
main application and contains copies of relevant input sources for each project
that is to be tested.
Note that the tests are sensitive to where they are invoked and typically need to be 
run from the application root directory.  

The tests are invoked automatically by the test package
```
{root}/pytest.sh
```

The examples in this directory can also be run outside of this test environment by using:
```
{root}/run_test_npn.sh
{root}/run_test_pep725.sh
