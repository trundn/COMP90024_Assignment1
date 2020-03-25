#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --job-name=twitter_one_node_one_core
#SBATCH --time=0-12:00:00
#SBATCH --partition=physical

# Load required modules
module load Python/3.7.1-GCC-6.2.0

# Launch twitter analyzer process
echo "Analyzing twitter data..."
time mpiexec python3 TwitterAnalyzer.py
