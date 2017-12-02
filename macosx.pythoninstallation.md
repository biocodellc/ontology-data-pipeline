# On MacOSX this can be installed with brew, e.g.
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
