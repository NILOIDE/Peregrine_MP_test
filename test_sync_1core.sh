#!/bin/bash
#SBATCH --time=0-00:10:00
#SBATCH --mem=1000
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
#SBATCH --output=test_sync_1core.out
python ./peregrine_test_synchronized.py
