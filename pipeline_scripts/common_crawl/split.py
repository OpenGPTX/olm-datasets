from datasets import load_dataset, load_from_disk, concatenate_datasets
import multiprocessing as mp 
import argparse
import os



parser = argparse.ArgumentParser(description="Combines cleaned chunks into one Dataset")
parser.add_argument("--input_dir", help="The directory of the .parquet files", required=True)
parser.add_argument("--output_dir", help="The name of the Hugging Face dataset which will be saved upon completion of this program.", required=True)
parser.add_argument("--split_percentage", help="The percentage of splitting to train and validation/test.", required=True)

args = parser.parse_args()



if __name__ == "__main__":
   
    ds = load_from_disk(args.input_dir)

    ds = ds.train_test_split(test_size=float(args.split_percentage))
        
    ds.save_to_disk(args.output_dir)

# to execute script e.g: python split.py --input_dir=test_dataset --output_dir=split_dataset --split_percentage=0.8



