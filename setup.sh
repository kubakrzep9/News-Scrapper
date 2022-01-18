#!/bin/bash
# may need to "chmod +x setup.sh"

# creating virtual environment to install packages in
VIR_ENV_NAME=news_scrapper
conda create -n $VIR_ENV_NAME python=3.8
source activate $VIR_ENV_NAME
# installing packages and project in virtual environment
pip install .
