#!/bin/bash
#SBATCH --partition=physical
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --time=0-00:20:00
#SBATCH --output=1node1core.out

module load Python/3.4.3-goolf-2015a
time mpirun -np 1 python TwitterCount.py
