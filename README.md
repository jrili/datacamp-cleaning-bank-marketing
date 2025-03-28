# Data Cleaning Project: Bank Marketing Campaign
_Instructions and dataset taken from Datacamp's [Cleaning Bank Marketing Campaign Data](https://app.datacamp.com/learn/projects/1613)_

# Links
|     Item       |   Link   |
| -------------- | ---------|
|Course Link | [DataCamp: Cleaning Bank Marketing Campaign Data](https://app.datacamp.com/learn/projects/1613) |
| Dataset | bank_marketing.csv |
| Author's Data Engineer Portfolio | [jrili/data-engineer-portfolio](https://github.com/jrili/data-engineer-portfolio) |

# Scenario
_(Directly taken from project instructions)_

You have been asked to work with a bank to clean the data they collected as part of a recent marketing campaign, which aimed to get customers to take out a personal loan. They plan to conduct more marketing campaigns going forward so would like you to ensure it conforms to the specific structure and data types that they specify so that they can then use the cleaned data you provide to set up a PostgreSQL database, which will store this campaign's data and allow data from future campaigns to be easily imported. 

They have supplied you with a csv file called `"bank_marketing.csv"`, which you will need to clean, reformat, and split the data, saving three final csv files. Specifically, the three files should have the names and contents as outlined below:

# Project Tasks

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

# How to execute script:
_TODO_
<!-- _(Tested in Python 3.13)_
```
python webscraping_movies.py
```
_Also available with sample outputs and explanations in notebook: [webscraping_top50films.ipynb](https://github.com/jrili/ibm-webscraping-films/blob/master/webscraping_top50films.ipynb)_ -->

# Acknowledgements
## Course Instructor
- George Boorman
## Course Offered By
* [DataCamp](https://app.datacamp.com/)
