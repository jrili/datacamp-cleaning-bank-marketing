Data Cleaning Project: Bank Marketing Campaign
=======================================
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Jupyter Notebook](https://img.shields.io/badge/jupyter-%23FA0F00.svg?style=for-the-badge&logo=jupyter&logoColor=white)
![Pandas](https://img.shields.io/badge/pandas-%23150458.svg?style=for-the-badge&logo=pandas&logoColor=white) 
[ETL]

***Part of a Data Engineer Portfolio: [jrili/data-engineer-portfolio](https://github.com/jrili/data-engineer-portfolio)***

# Project Description
This project focuses on extracting, cleaning, and transforming a bank marketing campaign dataset into structured, analysis-ready, and database-ready output files. It simulates a real-world ETL (Extract-Transform-Load) workflow using Python and Pandas.

A CSV file `"bank_marketing.csv"` is provided, which contain data that will need to be cleaned, reformatted, and split, saving three final csv files: `client.csv`, `campaign.csv`, and `economics.csv`.

# Project Objective
* Clean messy raw data (handling missing values, incorrect types, normalize field content formats)
* Perform tests at each stage to verify correctness of data processing
* Export clean datasets into multiple CSV files for analytics use

# Tools & Technologies Used
* Python 3.13
* Jupyter Notebook
* Pandas

# Specifications
## 1. Clean client data and load to `client.csv`
| column | data type | description | cleaning requirements |
|--------|-----------|-------------|-----------------------|
| `client_id` | `integer` | Client ID | N/A |
| `age` | `integer` | Client's age in years | N/A |
| `job` | `object` | Client's type of job | Change `"."` to `"_"` |
| `marital` | `object` | Client's marital status | N/A |
| `education` | `object` | Client's level of education | Change `"."` to `"_"` and `"unknown"` to `np.NaN` |
| `credit_default` | `bool` | Whether the client's credit is in default | Convert to `boolean` data type:<br> `1` if `"yes"`, otherwise `0` |
| `mortgage` | `bool` | Whether the client has an existing mortgage (housing loan) | Convert to boolean data type:<br> `1` if `"yes"`, otherwise `0` |

## 2. Clean campaign data and load to `campaign.csv`
| column | data type | description | cleaning requirements |
|--------|-----------|-------------|-----------------------|
| `client_id` | `integer` | Client ID | N/A |
| `number_contacts` | `integer` | Number of contact attempts to the client in the current campaign | N/A |
| `contact_duration` | `integer` | Last contact duration in seconds | N/A |
| `previous_campaign_contacts` | `integer` | Number of contact attempts to the client in the previous campaign | N/A |
| `previous_outcome` | `bool` | Outcome of the previous campaign | Convert to boolean data type:<br> `1` if `"success"`, otherwise `0`. |
| `campaign_outcome` | `bool` | Outcome of the current campaign | Convert to boolean data type:<br> `1` if `"yes"`, otherwise `0`. |
| `last_contact_date` | `datetime` | Last date the client was contacted | Create from a combination of `day`, `month`, and a newly created `year` column (which should have a value of `2022`); <br> **Format =** `"YYYY-MM-DD"` |

## 3. Clean economics data and load to `economics.csv`
| column | data type | description | cleaning requirements |
|--------|-----------|-------------|-----------------------|
| `client_id` | `integer` | Client ID | N/A |
| `cons_price_idx` | `float` | Consumer price index (monthly indicator) | N/A |
| `euribor_three_months` | `float` | Euro Interbank Offered Rate (euribor) three-month rate (daily indicator) | N/A |


# Workflow Overview
1. Extract the raw data from `bank_marketing.csv`
2. Process the **client-related data**
    * Extract and transform the client-related data from the raw data
    * Load the client-related data into `client.csv`
3. Process the **campaign-related data**
    * Extract and transform the campaign-related data from the raw data
    * Load the campaign-related data into `campaign.csv`
4. Process the **economics-related data**
    * Extract and transform the economics-related data from the raw data
    * Load the economics-related data into `campaign.csv`

# How to execute script:
_(Tested in Python 3.13)_
```
python clean-bank-marketing-data.py
```

_Also available with sample outputs and explanations in notebook: [datacamp-clean-bank-marketing.ipynb](https://github.com/jrili/datacamp-cleaning-bank-marketing/blob/master/datacamp-clean-bank-marketing.ipynb)_ 

# Key Learning Points
* Data extraction and validation using Pandas
* Data transformation techniqeus for real-world messy datasets
* Handling multiple output files to simulate loading into different targets

# Future Improvements
* Use test functions from noteook as basis for pytest scripts for validation
* Schedule ETL runs using Apache Airflow
* Load outputs directly to a relational database (PostgreSQL/MySQL)

# Acknowledgements
## Source Course
* [DataCamp: Cleaning Bank Marketing Campaign Data](https://app.datacamp.com/learn/projects/1613)
## Course Instructor
- George Boorman
