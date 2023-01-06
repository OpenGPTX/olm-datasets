#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=128
#SBATCH --time=24:00:00
#SBATCH --mem-per-cpu=1972
#SBATCH --partition=romeo
#SBATCH --output=get_hf_dataset.out
#SBATCH --error=get_hf_dataset.err
#SBATCH --account=p_gptx


export HF_DATASETS_CACHE="path/to/workspace"

source activate.sh #give absolute path here
python /beegfs/ws/0/s6690609-data-pipeline/olm-datasets/pipeline_scripts/common_crawl/get_hf_dataset_from_parquet.py --input_dir=/scratch/ws/0/s6690609-traindata/generated_corpuses/20221004 --output_dataset_name=hf_datasets --num_proc="$SLURM_CPUS_PER_TASK" 
