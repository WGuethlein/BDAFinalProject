#!/bin/bash

# Note: 
#   comments starting with #SBATCH are parsed by Slurm 
#   Ths is a draft! Please see Canvas for most up to date examples 


#SBATCH --job-name hadoop-example
#SBATCH --nodes=6 --ntasks-per-node=28
#SBATCH --time=01:00:00
#SBATCH --account PRS0021

# Load spark
module load spark/3.4.1


cp backend/ $TMPDIR
cp data/ $TMPDIR
cd $TMPDIR 


pbs-spark-submit server.py  > test.log

cp * $SLURM_SUBMIT_DIR