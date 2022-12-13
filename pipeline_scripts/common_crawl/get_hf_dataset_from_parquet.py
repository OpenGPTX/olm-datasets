from datasets import load_dataset
from tqdm import tqdm
import pandas as pd
import subprocess
from multiprocessing import Process
from os import walk, mkdir, path
from shutil import move, rmtree
import dateutil
import glob
import argparse

parser = argparse.ArgumentParser(description="Turns downloads from download_common_crawl.py into a Hugging Face dataset, split by language (language is identified using a FastText model). The dataset has a timestamp column for the time it was crawled, along with a url column and, of course, a text column.")
parser.add_argument("--input_dir", help="The directory of the .parquet files", required=True)
parser.add_argument("--output_dataset_name", help="The name of the Hugging Face dataset which will be saved upon completion of this program.", required=True)
parser.add_argument("--num_proc", type=int, help="The number of processes to use, at a minimum.", required=True)
parser.add_argument("--push_to_hub", action="store_true", help="Whether to push the Hugging Face dataset to the Hugging Face Hub after saving a copy to the disk.")
args = parser.parse_args()



#not parallelized reading of parquet files, #TODO use multiprocessing and splitting from get_text_dataset_from_wet_downloads.py for
ds = load_dataset(args.input_dir)

#from here serial
#TODO:have languges as an parsing argument and iterate over languages list, filter for every language and save
ds1 = ds.filter(lambda example: example["language"]=='de')
ds2 = ds.filter(lambda example: example["language"]=='en')
#TODO split datasets by language and create seperate language datasets
# ds.filter....
ds1.save_to_disk(args.output_dataset_name+'_de')
ds2.save_to_disk(args.output_dataset_name+'_en')

if args.push_to_hub:
    ds.push_to_hub(args.output_dataset_name)

#Goal file structure: (datasets[train[en[hf_ds],de[hf_ds]],[validation[en[hf_ds],de[hf_ds]]]]) 
#situation: just take parquet files and create hf dataset from it in one train and one val folder