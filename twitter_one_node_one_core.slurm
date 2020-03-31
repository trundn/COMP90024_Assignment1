#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --job-name=twitter_one_node_one_core
#SBATCH --time=00:50:00
#SBATCH --partition=physical

# Load required modules
module load Python/3.7.3-spartan_gcc-8.1.0

# Launch twitter analyzer process
echo "Analyzing twitter data..."
time mpiexec python3 TwitterAnalyzer.py -c languageConfig.json -d bigTwitter.json