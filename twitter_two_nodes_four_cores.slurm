#!/bin/bash
# 2 nodes, 4 tasks per node = 8 cores
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --cpus-per-task=1
#SBATCH --job-name=twitter_two_nodes_four_cores
#SBATCH --time=00:50:00
#SBATCH --partition=physical

# Load required modules
module load Python/3.7.1-GCC-6.2.0

# Launch twitter analyzer process
echo "Analyzing twitter data..."
time mpiexec python3 TwitterAnalyzer.py -c languageConfig.json -d bigTwitter.json
