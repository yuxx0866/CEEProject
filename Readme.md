# CEE Analysis Project

## Description
This project use the RECS Data 2020 to answer this question:

**How many single-family homes with central AC and heat with natural gas in Minnesota grouped by wall type?**

The answer to the follow up question is in follow up question answer.md

## Installation
This project use python and PostgresSQL to store and analyze the data. Therefore, you must have PostgresSQL and python installed before you run the code. 

Also, this project need you to install a few packages. you can use 

<code>pip install -r requirements.txt</code>

to install the required libraries.

### Dependencies
required libraries and packages are:
- pandas
- sqlalchemy
- psycopg2

### How to run the code
1. After you have all dependencies installed, please edit the config.json file so the information in it match your PostgresSQL's information. 
2. When you setup the config.json correctly, please run importAndAnalyze.py. It will start create a database and store data in different tables. Finally, it will print out a table that shows single-family homes with central AC and heat with natural gas in Minnesota grouped by wall type.

### Validation/qa checks performed
In this project, I perform three validation/qa checks. There are missing values and potential outliers in the dataset. However, since non of them affect answering the question correctly, I did not remove any rows or impute any missing values. 

The three validation/qa checks I performed are listed below:

- Check for missing values in the data
    - Among 21 observations that contain missing values, there are total of 63 missing values.
    - no missing values in columns that could answer the question. 
- Check for duplicates in the Data
    - No duplicate records in the Data
- Check for outlier based on standard deviation
    - There are 545 columns may contain outliers. The outlier data been saved to ./result/outlierList.csv.
    - However, without looking into the meaning of each variable, we should not directly remove outliers from data. These observations can contain very useful information that help us understand data better. 
    - If I have to remove outliers, I will remove them after talking to people with rich domain knowledge.

### Result analysis
The result been saved to ./result/result.csv. I also paste the result below in case there are something wrong in running the script.

| WALLTYPE                                         | counts |
|--------------------------------------------------|--------|
| Brick                                            | 21     |
| Concrete block                                   | 2      |
| Other                                            | 1      |
| Shingle(composition)                             | 9      |
| Siding(aluminum, fiber, cement, vinyl, or steel) | 436    |
| Stone                                            | 3      |
| Stucco                                           | 32     |
| Wood                                             | 100    |


### Contact Information
Email: guizhenyu0512@gmail.com