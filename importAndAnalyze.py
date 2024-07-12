# %%
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import json
import io

with open('config.json', 'r') as f:
    conn_params = json.load(f)

# %%
def create_database(dbname, user, password, host, port):
    # Connect to the default database (e.g., 'postgres')
    conn = psycopg2.connect(dbname='postgres', user=user, password=password, host=host, port=port)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)  # Needed to create a database
    cursor = conn.cursor()
    
    # Check if database exists and create if not
    cursor.execute("SELECT 1 FROM pg_database WHERE datname=%s", (dbname,))
    if cursor.fetchone():
        print(f"Database {dbname} already exists.")
    else:
        try:
            cursor.execute(f"CREATE DATABASE {dbname}")
            print(f"Database {dbname} created successfully.")
        except psycopg2.Error as e:
            print(f"An error occurred: {e}")
    
    cursor.close()
    conn.close()

# %%
def execute_sql_from_file(filename, connection):
    # Open and read the SQL file
    with open(filename, 'r') as file:
        sql_script = file.read()

    # Create a cursor to perform database operations
    cursor = connection.cursor()
    try:
        # Execute the SQL script
        cursor.execute(sql_script)
        connection.commit()  # Commit changes
        print(f"SQL script {filename} executed successfully.")
    except psycopg2.Error as e:
        print(f"An error occurred: {e}")
        connection.rollback()  # Roll back the transaction on error
    finally:
        cursor.close()

# %%
def upload_csv_to_sql(csv_file_path, database_url, table_name, if_exists_action='replace'):
    """
    Load CSV into a DataFrame and then upload it to a PostgreSQL database.

    Args:
    csv_file_path (str): Path to the CSV file.
    database_url (str): SQLAlchemy database URL.
    table_name (str): Name of the table where data will be inserted.
    if_exists_action (str): Action to take if the table already exists. Options are 'fail', 'replace', or 'append'.
    """
    # Create SQLAlchemy engine
    engine = create_engine(database_url)
    # Load data from CSV into DataFrame
    df = pd.read_csv(csv_file_path)

    df.head(0).to_sql(table_name, engine, if_exists='replace',index=False)
    # Upload data from DataFrame to SQL
    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    contents = output.getvalue()
    cur.copy_from(output, table_name, null="") # null values become ''
    conn.commit()
    cur.close()
    conn.close()
    print(f"{table_name}Data uploaded successfully.")


# %%
def query_postgres(sql_query, database_url):
    """
    Query the PostgreSQL database and return a DataFrame with the results.

    Args:
    sql_query (str): SQL query to be executed.
    database_url (str): SQLAlchemy database URL, e.g., 'postgresql://user:password@host:port/dbname'

    Returns:
    pd.DataFrame: DataFrame containing the query results.
    """
    # Create an SQLAlchemy engine
    engine = create_engine(database_url)

    # Query the database and return a DataFrame
    df = pd.read_sql_query(sql_query, engine)
    
    # Close the engine connection
    engine.dispose()

    return df

# %%
def check_missing_values(df):
    """
    Check and report missing values in the DataFrame.

    Args:
    df (pd.DataFrame): The DataFrame to check for missing values.

    Outputs:
    Prints the number of missing values, the number of rows affected by missing values, and the column names with missing values.
    """
    # Calculate total missing values
    total_missing = df.isnull().sum().sum()
    # Calculate number of rows with at least one missing value
    rows_with_missing = df.isnull().any(axis=1).sum()
    print(f"Among {rows_with_missing} observations that contain missing values, there are total of {total_missing} missing values.")

    # Find columns with missing values and their count
    columns_with_missing = df.isnull().sum()
    columns_with_missing = columns_with_missing[columns_with_missing > 0]
    print("Columns with missing values and their count:")
    print(columns_with_missing)
    print('---------------')

# %%
def check_for_duplicates(df):
    """
    Check for duplicate rows in the DataFrame and report findings.

    Args:
    df (pd.DataFrame): The DataFrame to check for duplicates.

    Outputs:
    Prints the number of duplicate rows and optionally lists them.
    """
    # Find duplicate rows, keeping the first occurrence as not a duplicate
    duplicate_rows = df[df.duplicated(keep='first')]

    # Count of duplicate rows
    num_duplicates = duplicate_rows.shape[0]
    print(f"Number of duplicate rows: {num_duplicates}")

    # print the duplicate rows if there is one
    if num_duplicates > 0:
        print("Duplicate rows:")
        print(duplicate_rows)
    
    print('---------------')

# %%
def detect_outliers_std(df):
    """
    Detect and count outliers in all numeric columns of a DataFrame using the 3 standard deviations method.
    
    Args:
    df (pd.DataFrame): The DataFrame to analyze.

    Outputs:
    Prints a table showing each numeric column with the count of outliers.
    """
    # Dictionary to hold outlier counts for each numeric column
    outlier_counts = {}

    # Iterate over each column in the DataFrame
    for column in df.select_dtypes(include=['number']).columns:
        # Calculate mean and standard deviation
        mean = df[column].mean()
        std_dev = df[column].std()

        # Define outliers as those outside of mean ± 3*std_dev
        lower_limit = mean - 3 * std_dev
        upper_limit = mean + 3 * std_dev
        outliers = df[(df[column] < lower_limit) | (df[column] > upper_limit)]
        
        # Count of outliers
        outlier_count = outliers.shape[0]
        
        # Store the count of outliers for the column
        if outlier_count > 0:
            outlier_counts[column] = outlier_count

    # Create a DataFrame from the dictionary to display the results in a tabular format
    outlier_summary = pd.DataFrame(list(outlier_counts.items()), columns=['Column', 'Outlier Count'])
    outlier_summary.to_csv('./result/outlierList.csv', index=False)
    print(f"There are {len(outlier_summary.index)} columns contain outliers. The outlier data been saved to ./result/outlierList.csv")

# %%
def main():
    # Database connection parameters
    user = conn_params['user']
    password = conn_params['password']
    host = conn_params['host']
    port = conn_params['port']
    dbname = 'ceedatabase_guizhen'
    conn_params['dbname'] = dbname
    database_url = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}'
    
    # Create database
    create_database(dbname, user, password, host, port)
    # Create conn
    conn = psycopg2.connect(**conn_params)

    # Execute existing script to create tables
    execute_sql_from_file('./sqlScripts/create_table.sql', conn)
    
    # upload data to database from csv file
    upload_csv_to_sql('./rawData/recs2020_public_v7.csv', database_url, "recs_data")
    #Upload type tables to database from csv files
    upload_csv_to_sql('./rawData/acequipm_pub_type.csv', database_url, "acequipm_pub_type")
    upload_csv_to_sql('./rawData/typehuq_type.csv', database_url, "typehuq_type")
    upload_csv_to_sql('./rawData/ugwarm_type.csv', database_url, "ugwarm_type")
    upload_csv_to_sql('./rawData/walltype_type.csv', database_url, "walltype_type")
    print('---------------')

    with open('./sqlScripts/query_all_recs_data.sql', 'r') as file:
        sql_query = file.read()
    #sql_query = 'SELECT * FROM recs_data;'
    rawData = query_postgres(sql_query, database_url)
    #Remove any records where heating degree days are less than 7000
    cleanData = rawData[rawData['HDD65']>=7000]
    #Check for missing values in the data
    check_missing_values(cleanData)
    #Check for duplicates in the Data
    check_for_duplicates(cleanData)
    #Check for outlier
    detect_outliers_std(cleanData)
    cleanData.to_csv('./result/cleanData.csv', index = False)

    # Select specific columns to answer the question
    selected_columns = ['ACEQUIPM_PUB', 'UGWARM', 'TYPEHUQ', 'WALLTYPE']  # List of column names to select
    selectedData = cleanData[selected_columns]
    selectedData = selectedData[
    (selectedData['ACEQUIPM_PUB'] == "Central air conditioner (includes central heat pump)") &
    (selectedData['UGWARM'] == "Yes") &
    (selectedData['TYPEHUQ'].str.contains("Single-family"))]
    print('---------------')
    # Group by 'WALLTYPE' and count occurrences
    grouped_data = selectedData.groupby('WALLTYPE').size().reset_index(name='counts')

    # Print the grouped DataFrame (optional)
    print('single-family homes with central AC and heat with natural gas in Minnesota grouped by wall type is shown below:')
    print(grouped_data)
    grouped_data.to_csv('./result/result.csv', index = False)


if __name__ == '__main__':
    main()


