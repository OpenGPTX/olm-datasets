#!/usr/bin/env bash

set -euo pipefail

echo 'starting setup for data preprocessing pipeline...'

#load needed modules
module load GCCcore/11.3.0 Python/3.9.7
module load CMake/3.23.1
#load Rust directly from Taurus instead of installing it
module load Rust

#clone repo if not already done
if [ ! -d "olm-datasets" ]; then
    git clone --recursive git@github.com:OpenGPTX/olm-datasets.git
fi

#create or activate virtual env
if [ -d "data-pipeline-env" ]; then
    echo "Virtual env already existing. Activating data-pipeline-env" 
    source data-pipeline-env/bin/activate
else
    virtualenv --system-site-packages data-pipeline-env
    #activate env 
    source data-pipeline-env/bin/activate
    pip install --upgrade pip
fi

#setup from repo
cargo install ungoliant@1.2.3
# reinstall in activated env to prevent using module from /home 
pip install -r olm-datasets/requirements.txt 

echo 'DONE!'