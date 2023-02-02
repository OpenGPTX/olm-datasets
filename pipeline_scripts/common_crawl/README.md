![olm_cc_pipeline](https://user-images.githubusercontent.com/20826878/199851707-64a7a026-c413-4d78-8b04-a825e07534b3.jpeg)

# Adapted Piepeline for OpenGPTX Datasets
<p align="center">
  <img src="https://github.com/OpenGPTX/olm-datasets/blob/multiprocessing/pipeline_scripts/common_crawl/prep-pipeline.drawio.svg"/>
</p>
<p align="center">
  <img src="https://github.com/OpenGPTX/olm-datasets/blob/multiprocessing/pipeline_scripts/common_crawl/manual_chunking.drawio.png"/>
</p>

<!-- ![preprocessing pipeline](https://github.com/OpenGPTX/olm-datasets/blob/multiprocessing/pipeline_scripts/common_crawl/prep-pipeline.drawio.svg)

![preprocessing pipeline with manual chunking](https://github.com/OpenGPTX/olm-datasets/blob/multiprocessing/pipeline_scripts/common_crawl/manual_chunking.drawio.png) -->

# Quick start
This section provides all the commands that you need to generate a deduplicated and filtered dataset from Common Crawl, ready for pretraining!
## Start on HPC system Taurus
Please make sure you've done the `setup.py` and followed all the instruction in the root dir of this repo before you start with preprocessing!
Before every use, activate the needed environment by `source activate.sh` directly from your workspace dir.
Detailed Informations can be found [here](https://github.com/OpenGPTX/olm-datasets).

## One time only

`bash download_pipeline_processing_models.sh`

## Every time

### On Taurus

### Interactive Execution of code (on Shell)
Allocate Resources - You will allloacte a complete node here! If you want to test things on small datasets, use only ``
```
srun --pty --partition romeo --ntasks=1 --cpus-per-task=128 --time=2:00:00 --mem-per-cpu=1972 bash -l
cd path/to/workspace
source activate.sh
```

Use the following commands to get a dataset. They should take only a few min if you have lots of CPUs. Adjust `--num_proc` to be equal to however many CPUs that you have.
For creating the raw HF dataset and removing Wikipedia URLS (if needed) do the following:

```
python get_hf_dataset_from_parquet.py --input_dir=/scratch/ws/0/s6690609-traindata/generated_corpuses/20221121 --output_dataset_name=cc_raw --num_proc=128 
python remove_wikipedia_urls.py --input_dataset_name=cc_raw --output_dataset_name=cc_no_wikipedia --url_column=source --split=train --num_proc=128

```
Cleaning and filtering of the data afterwards via the Crowdsourced data filtering from BigScience with all currently working filters and mappings:

```
python data-preparation/preprocessing/training/01a_catalogue_cleaning_and_filtering/clean.py \
--dataset-path=hf_dataset_de  \
--load-arrow-file \
--preprocessings "replace_newline_with_space" "remove_lines_with_code" "remove_html_spans" "remove_html_spans_sanad" "remove_wiki_mojibake" "strip_substrings_en_wiktionary" "filter_remove_empty_docs"  "filter_small_docs" \
--save-path=cc_filtered \
--batch-size=5
```
Latest, the deduplication used, is agian the Bloom deduplication used for OSCAR.
```
ulimit -Sn 1000000 && python deduplicate.py --input_dataset_name=cc_filtered --output_dataset_name=cc_olm --text_column=text --remove_whole_example --num_proc=224
```

Optionally, you can implement the current entire pipeline at once by using `run_pipeline.sh` (please make sure to check all the parameters and setup all the required pre-requisites)
```
sh run_pipeline.sh
```
### Batch based Implementation 

You can directly execute the pipeline code by running the `data_pipeline.sh` script (after reading it and modifying the inputs) as follows: 

```
sbatch data_pipeline.sh

```