#!/bin/bash
#SBATCH --time=00:10:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --partition=snowy
#SBATCH -o output_1n1c.txt

module load foss/2019b
module load python/3.7.4

srun -n 1 python3 ./a1_v1.py