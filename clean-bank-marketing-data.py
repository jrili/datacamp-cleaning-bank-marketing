# for data frame compilation and utilities
import pandas as pd

# for numerical computations and types
import numpy as np

# for deleting the output files, if existing, before the whole ETL process is run
import os, glob

from src.util import util
from src.util import constants as consts
from src.client_data import client_data_processing as client_data

if __name__ == "__main__":
    # Delete output files, if existing, to start each run with a clean slate
    util.clear_output_files()

    # Load input CSV file into a dataframe df
    print(f"Reading input CSV file '{consts.PATH_TO_INPUT_CSV_FILE}'...\n")
    df = pd.read_csv(consts.PATH_TO_INPUT_CSV_FILE)

    # Process client data and store to CSV file
    print("Processing client data...")
    client_data.process_data(df, consts.PATH_TO_CLIENT_CSV_FILE)
    print("Done processing client data!\n")
    
