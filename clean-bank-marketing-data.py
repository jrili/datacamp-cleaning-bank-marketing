# for data frame compilation and utilities
import pandas as pd

# for early exit upon encountering an error
import sys

# local imports
from src import util
from src import config
from src import client_data_processing as client_data
from src import campaign_data_processing as campaign_data
from src import economics_data_processing as econ_data

if __name__ == "__main__":
    # Delete output files, if existing, to start each run with a clean slate
    util.clear_output_files()

    # Load input CSV file into a dataframe df
    print(f"Reading input CSV file '{config.PATH_TO_INPUT_CSV_FILE}'...\n")
    df = pd.DataFrame()
    try:
        df = pd.read_csv(config.PATH_TO_INPUT_CSV_FILE)
    except FileNotFoundError as e:
        print(f"ERROR: File not found: {config.PATH_TO_INPUT_CSV_FILE}")
        sys.exit(1)

    # Process client data and store to CSV file
    print("Processing client data...")
    client_data.process_data(df, config.PATH_TO_CLIENT_CSV_FILE)
    print("Done processing client data!\n")
    
    # Process campaign data and store to CSV file
    print("Processing campaign data...")
    campaign_data.process_data(df, config.PATH_TO_CAMPAIGN_CSV_FILE)
    print("Done processing campaign data!\n")

    # Process economics data and store to CSV file
    print("Processing economics data...")
    econ_data.process_data(df, config.PATH_TO_ECONOMICS_CSV_FILE)
    print("Done processing economics data!\n")
