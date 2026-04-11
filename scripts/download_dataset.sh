#!/bin/bash
# Download only the Facebook dataset

set -e  # Exit on error

# Create data directory
mkdir -p data

# Define dataset
FB_DATASET="data/fb_100M_public_uint64"
BOOKS_DATASET="data/books_100M_public_uint64"
OSMC_DATASET="data/osmc_100M_public_uint64"


# Download if not already present
if [ ! -f $FB_DATASET ]; then
    echo "Downloading Facebook dataset (may take a while)..."
    wget -O $FB_DATASET "https://www.dropbox.com/scl/fi/hngvfbz1a2tkwpebjngb9/fb_100M_public_uint64?rlkey=px31l6wj9tnic4z604bt6s55n&st=d3iuhhgx&dl=0"
    echo "Download complete!"
else
    echo "Facebook dataset already exists, skipping download."
fi

# # Download books dataset
# if [ ! -f $BOOKS_DATASET ]; then
#     echo "Downloading books dataset (may take a while)..."
#     wget -O $BOOKS_DATASET "https://www.dropbox.com/scl/fi/q9zg3shi16xduo7mis3t8/books_100M_public_uint64?rlkey=f6bhaibqsugmo2yo2l4ir3v2r&st=tin2fnoj&dl=0"
#     echo "Download complete!"
# else
#     echo "Books dataset already exists, skipping download."
# fi

# # Download osm_cellids dataset
# if [ ! -f $OSMC_DATASET ]; then
#     echo "Downloading osm_cellids dataset (may take a while)..."
#     wget -O $OSMC_DATASET "https://www.dropbox.com/scl/fi/mamy2obtasfm898l0lrj1/osmc_100M_public_uint64?rlkey=vlzl311wa1q0cwsnc0nfow1qc&st=b30vck9r&dl=0"
#     echo "Download complete!"
# else
#     echo "osm_cellids dataset already exists, skipping download."
# fi

