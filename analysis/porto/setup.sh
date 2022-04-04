#!/bin/bash

ROOT_URL=https://archive.ics.uci.edu/ml/machine-learning-databases/00339/
FILES=("solution_challengeII.csv" "Porto_taxi_data_test_partial_trajectories.csv" "solution_fixed.csv" "train.csv.zip")
NUM_FILES=${#FILES[@]}
echo "Fetching dataset"
mkdir -p data
cd data
for ((i=1; i<$NUM_FILES+1; i++ ));
do
    FILE=${FILES[$i-1]}
    echo "\n\n($i/$NUM_FILES) -- $FILE"
    curl -o $FILE "$ROOT_URL$FILE"
done
unzip train.csv.zip
rm train.csv.zip
cd ..