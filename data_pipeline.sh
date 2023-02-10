#!/bin/bash
#SBATCH --job-name=data_pipeline
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1          # crucial - only 1 task per dist per node!
#SBATCH --cpus-per-task=32         # number of cores per tasks
#SBATCH --partition=julia
#SBATCH --time 2:00:00              # maximum execution time (HH:MM:SS)
#SBATCH --output=data_pipeline-%j.out           # output file name
#SBATCH --account=p_gptx
#SBATCH --mail-user=hammam.abdelwahab@mailbox.tu-dresden.de


# Install all dependencies before you execute the pipeline 
cd ..
source activate.sh

while getopts d:f:l: flag
do
    case "${flag}" in
        f) FILE_DIR=${OPTARG};;
        d) WORKING_DIR=${OPTARG};;
        l) LANGUAGE=${OPTARG};;
    esac
done

echo "Working Directory for Preprocessing: $WORKING_DIR"
echo "Dataset to be Preprocessed: $FILE_DIR"

#please change the directory to your corresponding workspace 
export  HF_DATASETS_CACHE=$WORKING_DIR/cache

cd $WORKING_DIR/olm-datasets/pipeline_scripts/common_crawl

#create folder for in between datasets
TMP_DATASETS=$WORKING_DIR/tmp_datasets
mkdir -p $TMP_DATASETS

PROC=$(($SLURM_JOB_CPUS_PER_NODE*$SLURM_NNODES))

# #split parquet files into smaller chunks
echo "_______________Creating Chunks____________________"
python create_parquet_chunks.py --input_dir=$FILE_DIR

echo "_______________Convert Parquet Files______________"
# #convert parquet files (per chunk) into hugging face files 
python get_hf_dataset_from_parquet.py --input_dir=$WORKING_DIR/olm-datasets/pipeline_scripts/common_crawl/chunks --output_dataset_name=$TMP_DATASETS/parquetdata --num_proc=$PROC

echo "_______________Remove Wikipedia URLs______________"
#remove wikipedia urls e.g german dataset (to be reviewed) 
python remove_wikipedia_urls.py --input_dataset_name=$TMP_DATASETS/parquetdata/results/$LANGUAGE --output_dataset_name=$TMP_DATASETS/dataset_without_wikipedia_url_de --url_column=source --split=train --num_proc=$PROC


#filtering (to be reviewed) 
echo "_______________Filter Dataset_____________________"
python data-preparation/preprocessing/training/01a_catalogue_cleaning_and_filtering/clean.py --dataset-path=$TMP_DATASETS/dataset_without_wikipedia_url_de --load-arrow-file --preprocessings "replace_newline_with_space" "remove_lines_with_code" "remove_html_spans" "remove_html_spans_sanad" "remove_wiki_mojibake" "strip_substrings_en_wiktionary" "filter_remove_empty_docs"  "filter_small_docs" --save-path=$TMP_DATASETS/dataset_filtered --batch-size=5

#merge cleaned chunks back into one dataset 
echo "______________Merge Chunks together_______________"
python merge_files.py --input_dir=$TMP_DATASETS/dataset_filtered --output_dir=$TMP_DATASETS/merged_composed_dataset

#Deduplication (to be reviewed) 
echo "______________Deduplication_______________________"
ulimit -Sn 1000000 && python deduplicate.py --input_dataset_name=$TMP_DATASETS/merged_composed_dataset --output_dataset_name=$TMP_DATASETS/deduplicated_composed_dataset --text_column=text --remove_whole_example --num_proc=$PROC

#Split data 
echo "______________Train/Validation data Split_________"
python split.py --input_dir=$TMP_DATASETS/deduplicated_composed_dataset --output_dir=$WORKING_DIR/final_dataset --split_percentage=0.8
