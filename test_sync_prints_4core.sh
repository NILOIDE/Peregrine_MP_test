#!/bin/bash
#SBATCH --time=0-00:10:00
#SBATCH --mem=1000
#SBATCH --nodes=1
#SBATCH --cpus-per-task=4
#SBATCH --output=test_sync_prints_4core.out
python ./peregrine_test_synchronized_with_prints.py
