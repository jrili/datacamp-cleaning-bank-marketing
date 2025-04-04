# for data frame compilation and utilities
import pandas as pd

# for numerical computations and types
import numpy as np

# local imports
from src.util import util

#################################################################################################
# Economics data assertion checks
#################################################################################################

def check_data(df: pd.DataFrame, old_df: pd.DataFrame):
    """Perform necessary checks as required for the economics data in the specifications
    """
    # Check number of rows
    assert(df.shape[0] == old_df.shape[0])

    # Check 'client_id' column
    assert('client_id' in df.columns)
    assert(df['client_id'].dtype == int)

    # Check 'cons_price_idx' column
    assert('cons_price_idx' in df.columns)
    assert(df['cons_price_idx'].dtype == float)

    # Check 'euribor_three_months' column
    assert('euribor_three_months' in df.columns)
    assert(df['euribor_three_months'].dtype == float)


#################################################################################################
# Economics data processing functions
#################################################################################################

def load_data(from_df: pd.DataFrame) -> pd.DataFrame:
    """Load the required economics data columns and clean them as per specifications

    Keyword Arguments:
    - from_df -- input dataframe loaded from input CSV referred to by PATH_TO_INPUT_CSV_FILE

    Return Values:
    - Dataframe loaded with the required economics data and data cleaning done
    """
    economics_df = from_df[['client_id', 'cons_price_idx', 'euribor_three_months']]

    return economics_df

def process_data(from_df: pd.DataFrame, output_filename):
    """Loads, checks, and saves the required data into the CSV file with specified filename

    Keyword Arguments:
    from_df -- -- input dataframe loaded from input CSV referred to by PATH_TO_INPUT_CSV_FILE
    output_filename -- filename of output CSV file

    Return Values:
    None
    """
    print("\tLoading and processing economics data...")
    economics_df = load_data(from_df)
    print("\tDone loading and processing economics data!\n")

    print("\tChecking processed economics data...")
    check_data(economics_df, from_df)
    print("\tDone checking processed economics data!\n")

    print(f"\tWriting processed economics data into '{output_filename}'...")
    economics_df.to_csv(output_filename, index=False)
    print(f"\tDone writing economics data into '{output_filename}'!\n")
