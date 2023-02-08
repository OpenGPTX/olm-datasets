#!/bin/bash
#SBATCH --job-name=data_pipeline
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1          # crucial - only 1 task per dist per node!
#SBATCH --cpus-per-task=128         # number of cores per tasks
#SBATCH --partition=julia
#SBATCH --time 2:00:00              # maximum execution time (HH:MM:SS)
#SBATCH --output=data_pipeline.out           # output file name
#SBATCH --account=p_gptx
#SBATCH --mail-user=hammam.abdelwahab@mailbox.tu-dresden.de


# Install all dependencies before you execute the pipeline 
source activate.sh

while getopts d:f: flag
do
    case "${flag}" in
        f) FILE_DIR=${OPTARG};;
        d) WORKING_DIR=${OPTARG};;
    esac
done

echo "Working Directory for Preprocessing: $WORKING_DIR"
echo "Dataset to be Preprocessed: $FILE_DIR"



#please change the directory to your corresponding workspace 
export  HF_DATASETS_CACHE=$WORKING_DIR/cache

cd $WORKING_DIR/olm-datasets/pipeline_scripts/common_crawl

# #split parquet files into smaller chunks
python create_parquet_chunks.py --input_dir=$FILE_DIR

# #convert parquet files (per chunk) into hugging face files 
python get_hf_dataset_from_parquet.py --input_dir=$WORKING_DIR/olm-datasets/pipeline_scripts/common_crawl/chunks --output_dataset_name=cc_raw --num_proc=128


# #remove wikipedia urls e.g german dataset (to be reviewed) 
python remove_wikipedia_urls.py --input_dataset_name=/beegfs/ws/1/haab446e-Dataset-pipe/olm-datasets/pipeline_scripts/common_crawl/results/de --output_dataset_name=composed_dataset_without_wikipedia_url_de --url_column=source --split=train --num_proc=128


#filtering (to be reviewed) 
python data-preparation/preprocessing/training/01a_catalogue_cleaning_and_filtering/clean.py --dataset-path=composed_dataset_without_wikipedia_url_de/beegfs/ws/1/haab446e-Dataset-pipe/olm-datasets/pipeline_scripts/common_crawl/results/de  --load-arrow-file --preprocessings "replace_newline_with_space" "remove_lines_with_code" "remove_html_spans" "remove_html_spans_sanad" "remove_wiki_mojibake" "strip_substrings_en_wiktionary" "filter_remove_empty_docs"  "filter_small_docs" --save-path=cc_filtered --batch-size=5

#merge cleaned chunks back into one dataset 
python merge_files.py --input_dir=composed_dataset_without_wikipedia_url_de/beegfs/ws/1/haab446e-Dataset-pipe/olm-datasets/pipeline_scripts/common_crawl/results/de --output_dir=merged_composed_dataset

#Deduplication (to be reviewed) 
ulimit -Sn 1000000 && python deduplicate.py --input_dataset_name=merged_composed_dataset --output_dataset_name=deduplicated_composed_dataset --text_column=text --remove_whole_example --num_proc=128

#Split data 
python split.py --input_dir=deduplicated_composed_dataset --output_dir=final_dataset --split_percentage=0.8
