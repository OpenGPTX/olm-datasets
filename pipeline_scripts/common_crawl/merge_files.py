from datasets import load_dataset, concatenate_datasets
import multiprocessing as mp 
import argparse
import os



parser = argparse.ArgumentParser(description="Combines cleaned chunks into one Dataset")
parser.add_argument("--input_dir", help="The directory of the .parquet files", required=True)
parser.add_argument("--output_dir", help="The name of the Hugging Face dataset which will be saved upon completion of this program.", required=True)
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

def merge_chunks(input_dir,dataset_dir,results):
    results[0] = concatenate_datasets([results[0],load_dataset(os.path.join(dataset_dir,each_dir),split='train')])
    


if __name__ == "__main__":
    with mp.Manager() as manager:
        results = manager.list()
        processes = []
        chunks_dir = list_folders(args.input_dir)
        results.append(load_dataset(os.path.join(args.input_dir,chunks_dir[0]),split='train')) 
        for each_dir in chunks_dir[1:]:
            p = mp.Process(target=merge_chunks, args=(each_dir,args.input_dir,results))
            p.start()
            processes.append(p)
        for p in processes:
            p.join()   
        print(results)
        results[0].save_to_disk(args.output_dir)
        
# to execute script e.g: python merge_parquet_files.py --input_dir=test_dataset --output_dir=merged_dataset