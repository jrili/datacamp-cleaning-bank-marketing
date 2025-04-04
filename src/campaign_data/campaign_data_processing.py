# for data frame compilation and utilities
import pandas as pd

# for numerical computations and types
import numpy as np

# local imports
from src.util import util

#################################################################################################
# Campaign data assertion checks
#################################################################################################

def check_data(df: pd.DataFrame, old_df: pd.DataFrame):
    """Perform necessary checks as required for the campaign data in the specifications
    """
    # Check number of rows
    assert(df.shape[0] == old_df.shape[0])

    # Check 'client_id' column
    assert('client_id' in df.columns)
    assert(df['client_id'].dtype == int)

    # Check 'number_contacts' column
    assert('number_contacts' in df.columns)
    assert(df['number_contacts'].dtype == int)

    # Check 'contact_duration' column
    assert('contact_duration' in df.columns)
    assert(df['contact_duration'].dtype == int)

    # Check 'previous_campaign_contacts' column
    assert('previous_campaign_contacts' in df.columns)
    assert(df['previous_campaign_contacts'].dtype == int)

    # Check 'previous_outcome' column
    util.assert_column_mapped_to_bool(df=df, old_df=old_df, target_column_name='previous_outcome',
                                 val_mapped_to_true='success')
    assert(df['previous_outcome'].dtype == bool)

    # Check 'campaign_outcome' column
    util.assert_column_mapped_to_bool(df=df, old_df=old_df, target_column_name='campaign_outcome',
                                 val_mapped_to_true='yes')
    assert(df['campaign_outcome'].dtype == bool)

    # Check 'last_contact_date' column
    # -all values in 'last_contact_date' are dates in between 2022-01-01(bounded) and 2023-01-01(unbounded)
    # -`month` and `date` columns are not present anymore
    assert('last_contact_date' in df.columns)
    assert(df['last_contact_date'].dtype == 'datetime64[ns]')
    assert( ((df['last_contact_date'] >= "2022-01-01") & 
            (df['last_contact_date'] < "2023-01-01")).sum() == len(df) )
    assert( ("month" not in df.columns) and ("day" not in df.columns) )


#################################################################################################
# Campaign data processing functions
#################################################################################################

def load_campaign_lastcontactdate_data(from_df: pd.DataFrame) -> pd.DataFrame:
    result_df = from_df.copy()
    """Adds a new datetime column `last_contact_date` to the input dataframe `from_df` where:
        - year: constant "2022" for all entries
        - month: directly mapped from `month` column
        - day: directly mapped from `day` column
    
    Keyword Arguments:
    - from_df -- dataframe with 'day' column of data type integer, and 'month' of data type string (i.e. 'object')
        - in case either column does not exist, print warning and return input as-is

    Return Values:
    - dataframe with the `last_contact_date` column of type datetime whose values are taken from the `month` and `day` columns, while `year` is set to 2022
    """

    if ('day' in result_df.columns) and ('month' in result_df.columns):
        # Isolate the date-related columns into a new dataframe
        # This is done to avoid creating unecessary new columns in the final dataframe campaign_df
        last_contact_date_df = result_df[['day']]

        # Convert month string to integer equivalent, then store to 'month' column
        # Note the usage of `assign()` instead of the usual direct assignment, e.g., df['new_column'] = ...
        #   This is done to avoid the SettingWithCopyWarning.
        month_dict = {'jan':1, 'feb':2, 'mar':3, 'apr':4, 'may':5, 'jun':6,
                    'jul':7, 'aug':8, 'sep':9, 'oct':10, 'nov':11, 'dec':12}
        last_contact_date_df = last_contact_date_df.assign(month=result_df['month'].map(month_dict))

        # Set the year to 2022 for all entries
        last_contact_date_df = last_contact_date_df.assign(year=2022)

        # Create new column `last_contact_date` from `last_contact_date_df`
        # Note the usage of `assign()` instead of the usual direct assignment, e.g., df['new_column'] = ...
        #   This is done to avoid the SettingWithCopyWarning.
        result_df = result_df.assign(last_contact_date=
                                     pd.to_datetime(arg=last_contact_date_df,
                                                    format="%Y-%m-%d"))
        #result_df.loc[:,'last_contact_date'] = result_df['last_contact_date']

        # Finally, we can drop the uneeded columns `month` and `day`
        result_df = result_df.drop(columns = ['month', 'day'])
    else:
        print(f"WARNING: In load_campaign_lastcontactdate_data(): required columns 'day' or 'month' not found in input dataframe, nothing to clean.")

    return result_df

def load_data(from_df: pd.DataFrame) -> pd.DataFrame:
    """Load the required campaign data columns and clean them as per specifications

    Keyword Arguments:
    - from_df -- input dataframe loaded from input CSV referred to by PATH_TO_INPUT_CSV_FILE

    Return Values:
    - Dataframe loaded with the required campaign data and data cleaning done
    """
    campaign_df = from_df[['client_id', 'number_contacts', 'contact_duration',
                'previous_campaign_contacts', 'previous_outcome', 'campaign_outcome', 'month', 'day']]

    campaign_df = util.map_str_to_bool_in_column(input_df=campaign_df,
                                        target_column_name='previous_outcome',
                                        map_to_true_str='success')

    campaign_df = util.map_str_to_bool_in_column(input_df=campaign_df,
                                        target_column_name='campaign_outcome',
                                        map_to_true_str='yes')

    campaign_df = load_campaign_lastcontactdate_data(campaign_df)

    return campaign_df

def process_data(from_df: pd.DataFrame, output_filename):
    """Loads, checks, and saves the required data into the CSV file with specified filename

    Keyword Arguments:
    from_df -- -- input dataframe loaded from input CSV referred to by PATH_TO_INPUT_CSV_FILE
    output_filename -- filename of output CSV file

    Return Values:
    None
    """
    print("\tLoading and processing campaign data...")
    campaign_df = load_data(from_df)
    print("\tDone loading and processing campaign data!\n")

    print("\tChecking processed campaign data...")
    check_data(campaign_df, from_df)
    print("\tDone checking processed campaign data!\n")

    print(f"\tWriting processed campaign data into '{output_filename}'...")
    campaign_df.to_csv(output_filename, index=False)
    print(f"\tDone writing campaign data into '{output_filename}'!\n")
