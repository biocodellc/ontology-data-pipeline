#!/bin/bash
eval "$(pyenv init -)"
python -m pytest $1
