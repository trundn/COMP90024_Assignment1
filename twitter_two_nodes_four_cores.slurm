#!/bin/bash
# 2 nodes, 4 tasks per node = 8 cores
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --time=0-12:00:00
#SBATCH --partition=physical

# Load required modules
module load Python/3.5.2-goolf-2015a 

# Launch twitter analyzer process
echo "Analyzing twitter data..."
time mpiexec python3 TwitterAnalyzer.py
