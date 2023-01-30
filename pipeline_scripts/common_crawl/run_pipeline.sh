#!/bin/bash


#Before you execute the pipeline please make sure that you have already all the dependencies installed and activated
# from README.md  
#sh setup.sh 
#source activate.sh


#please change the directory to your corresponding workspace 
export  HF_DATASETS_CACHE=/beegfs/ws/1/haab446e-Dataset-pipe/

#split parquet files into smaller chunks
python create_parquet_chunks.py --input_dir=/scratch/ws/0/s6690609-traindata/generated_corpuses/20221121/train/

#convert parquet files (per chunk) into hugging face files 
python get_hf_dataset_from_parquet.py --input_dir=/beegfs/ws/1/haab446e-Dataset-pipe/olm-datasets/pipeline_scripts/common_crawl/chunks --output_dataset_name=cc_raw --num_proc=128


#remove wikipedia urls e.g german dataset (to be reviewed) 
python remove_wikipedia_urls.py --input_dataset_name=/beegfs/ws/1/haab446e-Dataset-pipe/olm-datasets/pipeline_scripts/common_crawl/results/de --output_dataset_name=composed_dataset_without_wikipedia_url_de --url_column=source --split=train --num_proc=128


#filtering (to be reviewed) 

python data-preparation/preprocessing/training/01a_catalogue_cleaning_and_filtering/clean.py --dataset-path=composed_dataset_without_wikipedia_url_de/beegfs/ws/1/haab446e-Dataset-pipe/olm-datasets/pipeline_scripts/common_crawl/results/de  --load-arrow-file --preprocessings "replace_newline_with_space" "remove_lines_with_code" "remove_html_spans" "remove_html_spans_sanad" "remove_wiki_mojibake" "strip_substrings_en_wiktionary" "filter_remove_empty_docs"  "filter_small_docs" --save-path=cc_filtered --batch-size=5



#Deduplication (to be reviewed) 

ulimit -Sn 1000000 && python deduplicate.py --input_dataset_name=composed_dataset_without_wikipedia_url_de/beegfs/ws/1/haab446e-Dataset-pipe/olm-datasets/pipeline_scripts/common_crawl/results/de/ --output_dataset_name=deduplicated_composed_dataset --text_column=text --remove_whole_example --num_proc=128





