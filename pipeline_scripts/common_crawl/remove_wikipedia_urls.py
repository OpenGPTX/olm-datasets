from datasets import load_dataset, load_from_disk
import argparse
import os 
from multiprocessing import Process, set_start_method, Pool
from itertools import repeat


parser = argparse.ArgumentParser(description="Removes all examples from a Hugging Face dataset if they have a Wikipedia URL. This script is intened to be used if you eventually want to merge the dataset with a Wikipedia snapshot. In that case, examples from Wikipedia in this dataset are redundant.")
parser.add_argument("--input_dataset_name", help="Input dataset name.", required=True)
parser.add_argument("--output_dataset_name", help="Output dataset name.", required=True)
parser.add_argument("--url_column", help="Name of the URL column of the dataset.", required=True)
parser.add_argument("--split", default=None, help="The split of the dataset to use. Some datasets don't have splits, so it is optional.")
parser.add_argument("--num_proc", type=int, help="The number of processes to use.")
parser.add_argument("--push_to_hub", action="store_true", help="Whether to push the output dataset to the Hugging Face hub after saving to the disk.")
parser.add_argument("--load_from_hub_instead_of_disk", action="store_true", help="Whether to load the input dataset by name from the Hugging Face hub. If this argument isn't specified then the input dataset will be loaded from a directory of the same name on the disk.")
args = parser.parse_args()


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


def data_wikipedia_filter(args, input_dir):
    input_dir = os.path.join(args.input_dataset_name,input_dir)
    if args.load_from_hub_instead_of_disk:
        if args.split is None:
            ds = load_dataset(input_dir)
        else:
            ds = load_dataset(input_dir, split=split)
    else:
        if args.split is None:
            ds = load_from_disk(input_dir)
        else:
            ds = load_from_disk(input_dir)[args.split]
    
    ds = ds.filter(lambda example: not example[args.url_column].startswith("https://en.wikipedia.org/wiki/"))
    
    save_path = os.path.join(args.output_dataset_name, os.path.basename(input_dir))
    ds.save_to_disk(save_path)

    if args.push_to_hub:
        ds.push_to_hub(args.output_dataset_name)

def main():
    set_start_method("spawn")
    chunks_dir = list_folders(args.input_dataset_name)
    with Pool(args.num_proc) as p:
            p.starmap(data_wikipedia_filter,zip(repeat(args),chunks_dir))
if __name__ == "__main__":
    main()
