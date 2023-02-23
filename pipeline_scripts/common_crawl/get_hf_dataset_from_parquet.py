from datasets import load_dataset
from tqdm import tqdm
import datasets
import pandas as pd
import subprocess
from multiprocessing import Process, Pool,set_start_method
from os import walk, mkdir, path
from shutil import move, rmtree
import dateutil
import glob
import argparse
import os
import concurrent
import time
import uuid

t0 = time.time()

parser = argparse.ArgumentParser(description="Turns downloads from download_common_crawl.py into a Hugging Face dataset, split by language (language is identified using a FastText model). The dataset has a timestamp column for the time it was crawled, along with a url column and, of course, a text column.")
parser.add_argument("--input_dir", help="The directory of the .parquet files", required=True)
parser.add_argument("--output_dataset_name", help="The name of the Hugging Face dataset which will be saved upon completion of this program.", required=True)
parser.add_argument("--num_proc", type=int, help="The number of processes to use, at a minimum.", required=True)
parser.add_argument("--push_to_hub", action="store_true", help="Whether to push the Hugging Face dataset to the Hugging Face Hub after saving a copy to the disk.")
args = parser.parse_args()



input_dir = args.input_dir
output_dir = args.output_dataset_name



def list_folders(directory):
    # Get a list of all the files in the directory
    all_files = os.listdir(directory)
    # Initialize an empty list to store the folders
    folders = []
    # Iterate through the files in the directory
    for file in all_files:
        # Check if the file is a directory
        if os.path.isdir(os.path.join(directory, file)):
            # If it is, append it to the list of folders
            folders.append(file)
    # Return the list of folders
    return folders
def list_parquet_files(input_dir):
    all_files = glob.glob(os.path.join(input_dir,'*.parquet'))
    return all_files

chunks_dir = list_parquet_files(input_dir)
print(chunks_dir)

def data_pipeline(chunk_dir,input_dir=input_dir):
    try:
        print("file_dir:",chunk_dir) 
        ds = load_dataset(input_dir,data_files=chunk_dir)
        ds_de = ds.filter(lambda example: example["language"]=='de')
        ds_en = ds.filter(lambda example: example["language"]=='en')
        ds_de.save_to_disk(output_dir+'/results/de/'+'de_'+str(uuid.uuid4()))
        ds_en.save_to_disk(output_dir+'/results/en/'+'en_'+str(uuid.uuid4()))

    except datasets.data_files.EmptyDatasetError:
        print(f"datasets.data_files.EmptyDatasetError for {input_dir+'/'+chunk_dir}")
        
        

# for each_dir in chunks_dir:
#         Process(target=data_pipeline, args=(each_dir,input_dir,)).start()


with Pool(args.num_proc) as p:
    print(p.map(data_pipeline, chunks_dir))   

t1 = time.time()
print(f"Performance time {t1-t0}")



