#!/bin/bash
ml purge 
ml modenv/hiera
module load GCCcore/11.3.0 Python/3.9.7
module load CMake/3.23.1
module load Rust
#create or activate virtual env
source data-pipeline-env/bin/activate

#Envrionment Variables needed
export WORKING_DIR=`pwd`
export STANZA_RES=$WORKING_DIR/stanza_models
export INDIC_RESOURCES_PATH=$WORKING_DIR/indic_nlp_resources
