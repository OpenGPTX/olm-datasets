from pathlib import Path
from os import walk
from multiprocessing import Process
import pyarrow.parquet as pq
import argparse
import os

parser = argparse.ArgumentParser(description="Turns downloads from download_common_crawl.py into a Hugging Face dataset, split by language (language is identified using a FastText model). The dataset has a timestamp column for the time it was crawled, along with a url column and, of course, a text column.")
parser.add_argument("--input_dir", help="The directory of the .parquet files", required=True)
args = parser.parse_args()


print('reading files as batches')


def list_files(directory):
    # Get a list of all the files in the directory
    all_files = os.listdir(directory)
    # Initialize an empty list to store the files
    files = []
    # Iterate through the files in the directory
    for file in all_files:
        # Check if the file is a file
        if os.path.isfile(os.path.join(directory, file)):
            # If it is, append it to the list of files
            files.append(file)
    # Return the list of files
    return files


def create_chunks(file,dataset_dir=args.input_dir):

    print('create chunks folder')
    chunks_dir = Path('chunks').absolute()
    if chunks_dir.exists():
        print('chunks folder already exist, files will be overwritten')
    else:
        chunks_dir.mkdir(mode=0o777)
    print('chunks folder ready')

    # for file in f:
    print('reading file {0} succeeded'.format(file))
    parquet_file = pq.ParquetFile(dataset_dir+file)
    chunk_file_dir = Path('chunks/'+file.split('.parquet')[0].split('.snappy')[0].split('/')[-1]).absolute()
    chunk_file_dir.mkdir(mode=0o777)

    for idx,batch in enumerate(parquet_file.iter_batches(batch_size=100000)):
        print("RecordBatch No.{}".format(idx))
        print("Batch type: ",type(batch))
        print("here you can do preprocessing steps")
        #chunk_idx_dir= Path(f"chunks/{file.split('.parquet')[0].split('.snappy')[0].split('/')[-1]}/chunk_{idx}").absolute()
        #chunk_idx_dir.mkdir(mode=0o777)


        writer = pq.ParquetWriter(f"chunks/{file.split('.parquet')[0].split('.snappy')[0].split('/')[-1]}/chunk_{idx}.parquet", batch.schema)

        writer.write_batch(batch)
        writer.close()
            
            
            
for each_file in list_files(args.input_dir):
        Process(target=create_chunks, args=(each_file,)).start()

