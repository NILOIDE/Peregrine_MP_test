#!/bin/bash
#SBATCH --time=0-00:10:00
#SBATCH --mem=1000
#SBATCH --nodes=1
#SBATCH --cpus-per-task=4
#SBATCH --output=test_full_com_keras_4core.out
module load TensorFlow/1.6.0-foss-2018a-Python-3.6.4
python ./peregrine_test_full_communication_keras.py
