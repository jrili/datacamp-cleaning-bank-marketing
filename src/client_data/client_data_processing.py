# for data frame compilation and utilities
import pandas as pd

# for numerical computations and types
import numpy as np

from src.util import util

#################################################################################################
# Client data assertion checks
#################################################################################################

def check_data(df: pd.DataFrame, old_df: pd.DataFrame):
    """Perform necessary checks as required for the client data in the specifications
    """

    # Check 'client_id' column
    assert('client_id' in df.columns)
    assert(df['client_id'].dtype == int)

    # Check 'age' column
    assert('age' in df.columns)
    assert(df['age'].dtype == int)

    # Check 'job' column
    util.assert_column_doesnot_contain(df, 'job', '.')
    assert(df['job'].dtype == object)

    # Check 'marital' column
    assert('marital' in df.columns)
    assert(df['marital'].dtype == object)

    # Check 'education' column
    util.assert_column_doesnot_contain(df, 'education', '.')
    assert(df['education'].dtype == object)

    # Check 'credit_default' column
    util.assert_column_mapped_to_bool(df=df, old_df=old_df, target_column_name='credit_default',
                                 val_mapped_to_true='yes')
    assert(df['credit_default'].dtype == bool)

    # Check 'mortgage' column
    util.assert_column_mapped_to_bool(df=df, old_df=old_df, target_column_name='mortgage',
                                 val_mapped_to_true='yes')
    assert(df['mortgage'].dtype == bool)

#################################################################################################
# Client data processing functions
#################################################################################################

def load_data(from_df: pd.DataFrame) -> pd.DataFrame:
    """Load the required client data columns and clean them as per specifications

    Keyword Arguments:
    - from_df -- input dataframe loaded from input CSV referred to by PATH_TO_INPUT_CSV_FILE

    Return Values:
    - Dataframe loaded with the required client data and data cleaning done
    """
    clients_df = from_df[['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']]
    clients_df = util.substitute_chars_in_column(input_df=clients_df, target_column_name='job',
                                            subst_char='.', with_char='_')
    clients_df = util.substitute_chars_in_column(input_df=clients_df, target_column_name='education',
                                            subst_char='.', with_char='_')
    clients_df = util.map_str_to_bool_in_column(input_df=clients_df,
                                        target_column_name='credit_default',
                                        map_to_true_str='yes')
    clients_df = util.map_str_to_bool_in_column(input_df=clients_df,
                                        target_column_name='mortgage',
                                        map_to_true_str='yes')

    return clients_df

def process_data(from_df: pd.DataFrame, output_filename):
    """Loads, checks, and saves the required data into the CSV file with specified filename

    Keyword Arguments:
    from_df -- -- input dataframe loaded from input CSV referred to by PATH_TO_INPUT_CSV_FILE
    output_filename -- filename of output CSV file

    Return Values:
    None
    """
    print("\tLoading and processing client data...")
    clients_df = load_data(from_df)
    print("\tDone loading and processing client data!\n")

    print("\tChecking processed client data...")
    check_data(clients_df, from_df)
    print("\tDone checking processed client data!\n")

    print(f"\tWriting processed client data into '{output_filename}'...")
    clients_df.to_csv(output_filename, index=False)
    print(f"\tDone writing client data into '{output_filename}'!\n")
