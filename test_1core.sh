#!/bin/bash
#SBATCH --time=0-00:03:00
#SBATCH --mem=10
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
#SBATCH --output=test_1core.out
python ./peregrine_test.py