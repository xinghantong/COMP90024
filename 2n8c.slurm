#!/bin/bash
#SBATCH --time=00:10:00
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --cpus-per-task=1
#SBATCH --partition=snowy
#SBATCH -o output_2n8c.txt

module load foss/2019b
module load python/3.7.4

srun -n 8 python3 ./a1_v1.py