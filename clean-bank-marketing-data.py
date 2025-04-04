# for data frame compilation and utilities
import pandas as pd

# for numerical computations and types
import numpy as np

# for deleting the output files, if existing, before the whole ETL process is run
import os, glob

from src.util import util
from src.util import constants as consts
from src.client_data import client_data_processing as client_data
from src.campaign_data import campaign_data_processing as campaign_data
from src.economics_data import economics_data_processing as econ_data

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
    
    # Process campaign data and store to CSV file
    print("Processing campaign data...")
    campaign_data.process_data(df, consts.PATH_TO_CAMPAIGN_CSV_FILE)
    print("Done processing campaign data!\n")

    # Process economics data and store to CSV file
    print("Processing economics data...")
    econ_data.process_data(df, consts.PATH_TO_ECONOMICS_CSV_FILE)
    print("Done processing economics data!\n")
