# for data frame compilation and utilities
import pandas as pd

# for numerical computations and types
import numpy as np

# for deleting the output files, if existing, before the whole ETL process is run
import os, glob

from src.util import constants as consts
#################################################################################################
# Data processing utility functions
#################################################################################################

def clear_output_files():
    print(f"Clearing output files in '{consts.OUTPUT_DIR}'...")
    for f in glob.glob(f"{consts.OUTPUT_DIR}/*.csv"):
        print(f"\tDeleting file {os.path.basename(f)}...")
        os.remove(f)
    print(f"Done clearing output files in '{consts.OUTPUT_DIR}'!\n")

def substitute_chars_in_column(input_df: pd.DataFrame, target_column_name: str,
                            subst_char: str, with_char: str) -> pd.DataFrame:
    """Cleans data in the `target_column_name` column by substituting `subst_char` occurrences with `with_char`
    
    Keyword Arguments:
    - input_df -- dataframe with a `target_column_name` column of data type string (i.e. 'object')
        - in case column does not exist, print warning and return input as-is

    Return Values:
    - dataframe with the `target_column_name` column substituted as specified
    """
    result_df = input_df.copy()
    if(target_column_name in result_df.columns):
        rows_to_subst = input_df[target_column_name].str.contains(f'\\{subst_char}')
        result_df.loc[ rows_to_subst, target_column_name] = result_df[ rows_to_subst ][target_column_name].str.replace(subst_char, with_char)
    else:
        print(f"WARNING: In substitute_chars_in_column(): '{target_column_name}' column not found in input dataframe, nothing to clean.")

    return result_df

def map_str_to_bool_in_column(input_df: pd.DataFrame, target_column_name: str,
                              map_to_true_str: str) -> pd.DataFrame:
    """Maps a string-type column `target_column_name` to boolean values with the ff. mapping:
        - `map_to_true_str` values -> True
        - other values -> False

    Keyword Arguments:
    - input_df -- dataframe containing a string-type column `target_column_name`
    - target_column_name -- the target column name where mapping should be done
    - map_to_true_str -- string value within `target_column_name` column that should be mapped to True

    Return Values:
    - dataframe with the `target_column_name` converted to boolean type with the specified mapping
    """
    result_df = input_df.copy()

    if(target_column_name in result_df.columns):
        # Rename the credit_default column to credit_default_obj to reflect its datatype
        obj_column_name = f"{target_column_name}_obj"
        result_df = result_df.rename(columns={target_column_name:obj_column_name})

        # Create new column with same name with boolean values depending on the renamed column
        result_df[target_column_name] = (result_df[obj_column_name] == map_to_true_str)

        # Drop the unneeded renamed column
        result_df = result_df.drop(columns=[obj_column_name])
    else:
        print(f"WARNING: In map_str_to_bool_in_column(): '{target_column_name}' column not found in input dataframe, nothing to clean.")


    return result_df

#################################################################################################
# Assertion check functions
#################################################################################################

def assert_column_doesnot_contain(df: pd.DataFrame, target_column_name: str,
                                contains_char: str):
    """Asserts `target_column_name` column exists in `df` and does not contain values with `contains_char` character
    """
    assert(target_column_name in df.columns)
    contains_char_rows = df[target_column_name].str.contains(f'\\{contains_char}')
    assert(contains_char_rows.sum() == 0)

def assert_column_mapped_to_bool(df: pd.DataFrame, old_df: pd.DataFrame,
                                 target_column_name: str, val_mapped_to_true: str):
    """Asserts the `target_column_name` column exists in `df` and that `val_mapped_to_true` values are mapped to True while the rest are mapped to False
    """
    assert(target_column_name in df.columns)
    assert((df[target_column_name] == True).sum() == (old_df[target_column_name] == val_mapped_to_true).sum())
    assert((df[target_column_name] == False).sum() == (old_df[target_column_name] != val_mapped_to_true).sum())
