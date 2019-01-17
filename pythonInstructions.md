# Notes on setting up python

However you go about running this application, it is reccomended to setup a clean virtual environment 
for your python to be sure you have the correct packages and version of python to run the pipeline.
At the very least, you will need python3+
The following are some notes on setting up your python environment to run the ontology-data-pipeline.

A useful resource is the [python virtual environments primer](https://realpython.com/python-virtual-environments-a-primer/)


# Debian/Ubuntu Method
(On Mac OSX,  you can use ```brew install```  to install packages, or with CentOS/Redhat systems you can try ```yum install```
You may need to preface some of the installation commands with ```sudo```)

```
# install pyenv if necessary
#installed using [pyenv](https://github.com/pyenv/pyenv)v
apt-get install pyenv

#You *may* need the next two lines to setup a /tmp directory that has exec permissions on for pyenv to work.  This is in case your system's /tmp directory has noexec turned on
mkdir ~/tmp
export TMPDIR="$HOME/tmp"

# install the proper version of python if necessary
pyenv init -
pyenv install 3.7.2
pyenv local 3.7.2

# install virtualenv if necessary
apt-get install python-virtualenv
# or...
pip install virtualenv

# setup your virtualenvironment directory if necessary
mkdir /home/myusername/virtualenvironment

# create the virtualenvironment
virtualenv --python=/home/myusername/.pyenv/versions/3.7.2/bin/python ~/virtualenvironment/ontology-data-pipeline
source ~/virtualenvironment/ontology-data-pipeline
```
You should see a prompt like: ```(ontology-data-pipeline) myusername@myhost:$```

