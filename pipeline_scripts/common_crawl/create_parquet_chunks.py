from pathlib import Path
from os import walk
from multiprocessing import Process, Pool, set_start_method
import pyarrow.parquet as pq
import argparse
import glob
from  itertools import repeat
import os
import uuid

def list_files(directory):
    # Get a list of all the files in the directory
    all_files = glob.glob(os.path.join(directory,"*.parquet"))
    # Initialize an empty list to store the files
    print(f"Found {len(all_files)} parquet files in total!")
    return all_files


def create_chunks(file, dataset_dir):
    # for file in f:
    print('reading file {0} succeeded'.format(file))
    parquet_file = pq.ParquetFile(file)
    chunk_file_dir = Path('chunks/'+file.split('.parquet')[0].split('.snappy')[0].split('/')[-1]).absolute()
    try: 
        chunk_file_dir.mkdir(mode=0o777)
    except FileExistsError:
        print("File exsists")
    print(file)
    for idx,batch in enumerate(parquet_file.iter_batches(batch_size=5000)):
        print("RecordBatch No.{}".format(idx))
        print("Batch type: ",type(batch))
        print("here you can do preprocessing steps")
        #chunk_idx_dir= Path(f"chunks/{file.split('.parquet')[0].split('.snappy')[0].split('/')[-1]}/chunk_{idx}").absolute()
        #chunk_idx_dir.mkdir(mode=0o777)


        writer = pq.ParquetWriter(f"chunks/{file.split('.parquet')[0].split('.snappy')[0].split('/')[-1]}/chunk_{uuid.uuid4()}.parquet", batch.schema)

        writer.write_batch(batch)
        writer.close()


def main():
    parser = argparse.ArgumentParser(description="Turns downloads from download_common_crawl.py into a Hugging Face dataset," \
                                                 "split by language (language is identified using a FastText model). The dataset" \
                                                 "has a timestamp column for the time it was crawled, along with a url column and, of course, a text column.")
    parser.add_argument("--input_dir", help="The directory of the .parquet files", required=True)
    args = parser.parse_args()

    print('reading files as batches')
    set_start_method("spawn")
    with  Pool(32) as p:
        print('create chunks folder')
        chunks_dir = Path('chunks').absolute()
        if chunks_dir.exists():
            print('chunks folder already exist')
        else:
            chunks_dir.mkdir(mode=0o777)
        print('chunks folder ready')
        
        files = list_files(args.input_dir)   
        print(files)
        p.starmap(create_chunks,zip(files,repeat(args.input_dir)))
        p.close()
        p.join()      

# for each_file in list_files(args.input_dir):
# Process(target=create_chunks, args=(each_file,)).start()

if __name__ == '__main__':
    main()

