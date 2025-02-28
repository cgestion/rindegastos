import time
from api_utils import check_api_availability
import json
import pandas as pd
from sqlalchemy import create_engine
import datetime
import unicodedata
import numpy as np
import pyodbc
import functools
import traceback
import requests
from socket import timeout
import os
from sunatinfo_target_columns import sunatinfo_target_columns
from dotenv import load_dotenv

load_dotenv()

# Database connection details
server = os.getenv('DB_SERVER')
database = os.getenv('DB_DATABASE')
schema_name = os.getenv('DB_SCHEMA')
username = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')
current_year = datetime.datetime.now().year-1
current_date = datetime.datetime.now().strftime("%Y-%m-%d")

# Function to establish the database connection
def get_database_connection():
    conn_str = (
        'DRIVER={SQL Server};SERVER=' + server +
        ';DATABASE=' + database +
        ';UID=' + '{' + username + '}' +
        ';PWD=' + '{' + password + '}'
    )
    return pyodbc.connect(conn_str)

# Decorator function to log exceptions and successes into the database
def log_exceptions(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        file_name = os.path.basename(func.__code__.co_filename)
        try:
            result = func(*args, **kwargs)
            # Log success
            success_message = f"Success in {func.__name__} (called from {file_name})"
            conn = get_database_connection()
            cursor = conn.cursor()
            cursor.execute('INSERT INTO fil.rindegastos_logs (date, log) VALUES (?, ?)', (datetime.datetime.now(), success_message))
            cursor.execute("EXEC [A_CONF].[titan].[enviar_alerta] ?, ?", ('miguel.saavedra', success_message))
            conn.commit()
            conn.close()
            return result
        except Exception as e:
            # Get the line number and traceback details
            tb = traceback.format_exc()
            error_message = f"Error in {func.__name__} (called from {file_name}): {e}\nTraceback: {tb}"
            # Store the error in the database
            conn = get_database_connection()
            cursor = conn.cursor()
            cursor.execute('INSERT INTO fil.rindegastos_logs (date, log) VALUES (?, ?)', (datetime.datetime.now(), error_message))
            error_message_short = f"Error in {func.__name__} (called from {file_name}): {e}"
            cursor.execute("EXEC [A_CONF].[titan].[enviar_alerta] ?, ?", ('miguel.saavedra', error_message_short))
            conn.commit()
            conn.close()
            if file_name == 'actualizar_informe_y_gastos_rindegastos.py':
                # This is so that the "Actualizar informe" button in "Informes rendiciones detalle" does not return a success message
                input("An error occurred. Press Enter to exit.")
    return wrapper

# Function to transform lists and dictionaries to strings
def transform_to_string(cell):
    if isinstance(cell, (list, dict)):
        return str(cell)
    else:
        return cell

def remove_accents_and_spaces(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    without_accents = ''.join([c for c in nfkd_form if not unicodedata.combining(c)])
    return without_accents.replace(' ', '_')

def fetch_and_store_sunatinfo_data(sunatinfo_df, target_table, status="1"):
    data_keys = set()
    nested_data_keys = set()
    
    for index, row in sunatinfo_df.iterrows():
        sunat_info = row['SunatInfo']
        if isinstance(sunat_info, dict):
            data_keys.update(sunat_info.keys())
            if 'extractedData' in sunat_info:
                try:
                    json_string = sunat_info['extractedData']
                    extracted_data = json.loads(json_string)
                    if isinstance(extracted_data, dict):
                        nested_data_keys.update(extracted_data.keys())
                    else:
                        extracted_data = json.loads(extracted_data)['data']
                        nested_data_keys.update(extracted_data.keys())
                except Exception as e:
                    print(f"Error processing 'extractedData': {e}")
                    
    # Initialize an empty DataFrame with specified columns
    sunat_info_columns = list(data_keys) + ['nested_' + col for col in nested_data_keys]
    df = pd.DataFrame(columns=['Id'] + sunat_info_columns)  # Add 'Id' column here
    
    # Iterate over rows of sunatinfo_df DataFrame
    for index, row in sunatinfo_df.iterrows():
        sunat_info = row['SunatInfo']
        new_row_data = {'Id': row.Id}  # Add 'Id' to new_row_data
        for key in data_keys:
            if isinstance(sunat_info, dict) and key in sunat_info:
                new_row_data[key] = sunat_info[key]
            else:
                new_row_data[key] = np.nan
        
        for key in nested_data_keys:
            nested_key = 'nested_' + key
            if isinstance(sunat_info, dict) and 'extractedData' in sunat_info:
                try:
                    extracted_data = json.loads(sunat_info['extractedData'])
                    if isinstance(extracted_data, dict) and key in extracted_data:
                        new_row_data[nested_key] = extracted_data[key]
                    elif isinstance(extracted_data, dict) and 'data' in extracted_data:
                        if extracted_data['data'] is not None:
                            new_row_data[nested_key] = extracted_data['data'].get(key)
                    else:
                        extracted_data = sunat_info['extractedData']
                        extracted_data = json.loads(extracted_data)
                        extracted_data = json.loads(extracted_data)
                        new_row_data[nested_key] = extracted_data['data'].get(key)
                except Exception as e:
                    new_row_data[nested_key] = np.nan
            else:
                new_row_data[nested_key] = np.nan
            
        # Assign the row data directly to the DataFrame
        df.loc[len(df)] = new_row_data
    
    df.fillna(np.nan, inplace=True)
    
    # Drop rows where all values are None type
    df.dropna(subset=df.columns, how='all', inplace=True)
    
    # df = df[target_columns]
    df.dropna(subset=df.columns.difference(['Id']), how='all', inplace=True)

    # Add suffix to column names that have nested as prefix
    new_columns = {col: col + '_nested' if col.startswith('nested_') else col for col in df.columns}
    df.rename(columns=new_columns, inplace=True) 
    
    # Remove prefix from column names
    new_columns = {col: col.replace('nested_', '') for col in df.columns}
    df.rename(columns=new_columns, inplace=True)

    connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    engine = create_engine(connection_string)

    # Apply transformation to each element in the DataFrame
    df = df.map(transform_to_string)
    df.drop_duplicates(inplace=True)

    target_columns = sunatinfo_target_columns
    df = df.reindex(columns=target_columns, fill_value=np.nan)

    df = df[target_columns]
    df['fecha_carga'] = datetime.datetime.now().strftime("%d-%m-%Y %H:%M")

    df.to_sql(target_table, engine, schema=schema_name, if_exists='append', index=False)

def fetch_and_store_extrafields_data(extrafields_df, target_table, status="1"):
    # Create an empty dictionary to store the extracted values
    extracted_values = {'Id': []}
    
    # Extract values for each desired field
    if target_table == 'rindegastos_gastos_extrafields':
        desired_fields = ['Impuesto', 'Centro Costo', 'Tipo Documento', 'RUC Proveedor', 'Serie', 'Correlativo', 'Comentario']
    elif target_table == 'rindegastos_informes_extrafields':
        desired_fields = ['Sede', 'Sociedad', 'Condición Pago', 'Tipo Rendición', 'Tipo Tasa', 'Vacio']
    for row in extrafields_df.itertuples(index=False):
        row_values = {field['Name']: field['Value'] for field in row.ExtraFields if field['Name'] in desired_fields}
        extracted_values['Id'].append(row.Id)
        for field in desired_fields:
            cleaned_field = remove_accents_and_spaces(field)
            if field in row_values:
                extracted_values[f"{cleaned_field}_Value"] = extracted_values.get(f"{cleaned_field}_Value", []) + [row_values[field]]
                extracted_values[f"{cleaned_field}_Code"] = extracted_values.get(f"{cleaned_field}_Code", []) + [next((f['Code'] for f in row.ExtraFields if f['Name'] == field), None)]
            else:
                extracted_values[f"{cleaned_field}_Value"] = extracted_values.get(f"{cleaned_field}_Value", []) + [None]
                extracted_values[f"{cleaned_field}_Code"] = extracted_values.get(f"{cleaned_field}_Code", []) + [None]
    # Replace empty strings with np.nan
    extracted_values = {k: [np.nan if v == '' else v for v in extracted_values[k]] for k in extracted_values}

    df = pd.DataFrame(extracted_values)

    connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    engine = create_engine(connection_string)

    # Apply transformation to each element in the DataFrame
    df = df.map(transform_to_string)
    df.drop_duplicates(inplace=True)
    df['fecha_carga'] = datetime.datetime.now().strftime("%d-%m-%Y %H:%M")

    df.to_sql(target_table, engine, schema=schema_name, if_exists='append', index=False)

MAX_RETRIES = 3
RETRY_DELAY = 5  # in seconds
LOG_FILE = "cargar_rindegastos_error_log.txt"  # Log file name

if not os.path.exists(LOG_FILE):
    with open(LOG_FILE, 'w') as f:
        pass
        
# Function to fetch data and store it in a DataFrame
def fetch_and_store_data(endpoint, target_table, data_key, status="1"):
    retries = 0
    while retries < MAX_RETRIES:
        try:
            all_data = []
            page = 1
            params = {}
            
            if target_table == "rindegastos_informes":
                if status == "1":
                    params["Since"] = f"{current_year}-01-01"
                else:
                    params["Status"] = status
            elif target_table == "rindegastos_gastos":
                params["Since"] = f"{current_year}-01-01"
                params["Until"] = f"{current_date}"
                params["Status"] = status
                
            params["ResultsPerPage"] = "999"
            
            while True:
                params["Page"] = str(page)
                result = endpoint(params)
                
                if result.status_code != 200:
                    print(f"HTTP Error {result.status_code}: Unable to fetch page {page}")
                    break
                
                response_data = result.text
                try:
                    data = json.loads(response_data)
                except json.JSONDecodeError:
                    print(f"Failed to decode JSON response for page {page}")
                    break
                
                records = data.get(data_key, [])
                all_data.extend(records)
                
                pages = data.get('Records', {}).get('Pages', 1)
                print(f"Processed page {page} out of {pages} for {data_key} with status {status}.")
                
                page += 1
                if page > pages:
                    break
            
            df = pd.DataFrame(all_data)
        
            if target_table == 'rindegastos_gastos': 
                # Extract Id and ExtraFields for separate processing and storage
                extrafields_df = df[['Id', 'ExtraFields']]
                # Extract Id and SunatInfo for separate processing and storage
                sunatinfo_df = df[['Id', 'SunatInfo']]
                
                if status == "1":
                    fetch_and_store_extrafields_data(extrafields_df, 'rindegastos_gastos_extrafields')
                    fetch_and_store_sunatinfo_data(sunatinfo_df, 'rindegastos_gastos_sunatinfo')
                else:
                    fetch_and_store_extrafields_data(extrafields_df, 'rindegastos_gastos_extrafields', "0")
                    fetch_and_store_sunatinfo_data(sunatinfo_df, 'rindegastos_gastos_sunatinfo', "0")
                    
            elif target_table == 'rindegastos_informes': 
                # Extract Id and ExtraFields for separate processing and storage
                extrafields_df = df[['Id', 'ExtraFields']]
                if status == "1":
                    fetch_and_store_extrafields_data(extrafields_df, 'rindegastos_informes_extrafields')
                else:
                    fetch_and_store_extrafields_data(extrafields_df, 'rindegastos_informes_extrafields', '0')
        
            connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
            engine = create_engine(connection_string)
        
            # Apply transformation to each element in the DataFrame
            df = df.map(transform_to_string)
            df.drop_duplicates(inplace=True)
            df['fecha_carga'] = datetime.datetime.now().strftime("%d-%m-%Y %H:%M")
        
            if status == "1":
                df.to_sql(target_table, engine, schema=schema_name, if_exists='append', index=False)
            else:
                if target_table == "rindegastos_gastos":
                    df = df[df['IssueDate'].str[:4].astype(int) >= 2024]
                else:
                    df = df[df['SendDate'].str[:4].astype(int) >= 2024]
                df.to_sql(target_table, engine, schema=schema_name, if_exists='append', index=False)
                
            # Log success
            with open(LOG_FILE, 'a') as f:
                f.write(f"{datetime.datetime.now()} - Successfully fetched and stored {data_key} with status {status} after {retries} retries\n")
            
            break  # Exit retry loop on success
        except (requests.exceptions.RequestException, timeout) as e:
            retries += 1
            time.sleep(RETRY_DELAY)
        except Exception as e:
            with open(LOG_FILE, 'a') as f:
                f.write(f"{datetime.datetime.now()} - Error: {e}\n")
            retries += 1
                
    if retries == MAX_RETRIES:
        with open(LOG_FILE, 'a') as f:
            f.write(f"Failed to fetch and store {data_key} with status {status} after {retries} retries\n")
            
# Function to delete records from various tables
def delete_rindegastos_gastos():
    print('Deleting current year gastos')
    conn = get_database_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(f"SELECT Id FROM ciclo_proveedores.fil.rindegastos_gastos WHERE IssueDate >= '{current_year}-01-01'")
        ids = [row.Id for row in cursor.fetchall()]

        if ids:
            cursor.execute(f"DELETE FROM ciclo_proveedores.fil.rindegastos_gastos_extrafields WHERE Id IN ({','.join(map(str, ids))})")
            cursor.execute(f"DELETE FROM ciclo_proveedores.fil.rindegastos_gastos_sunatinfo WHERE Id IN ({','.join(map(str, ids))})")
            cursor.execute(f"DELETE FROM ciclo_proveedores.fil.rindegastos_gastos WHERE Id IN ({','.join(map(str, ids))})")
            print("Deleted rindegastos_gastos records")
        else:
            print(f"No records found in rindegastos_gastos where IssueDate >= {current_year}")

        conn.commit()

    except Exception as e:
        print(f"Error deleting records: {e}")

    finally:
        cursor.close()
        conn.close()
        
def delete_rindegastos_informes():    
    print('Deleting current year informes')
    conn = get_database_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(f"SELECT Id FROM ciclo_proveedores.fil.rindegastos_informes WHERE SendDate >= '{current_year}-01-01'")
        ids = [row.Id for row in cursor.fetchall()]

        if ids:
            cursor.execute(f"DELETE FROM ciclo_proveedores.fil.rindegastos_informes_extrafields WHERE Id IN ({','.join(map(str, ids))})")
            cursor.execute(f"DELETE FROM ciclo_proveedores.fil.rindegastos_informes WHERE Id IN ({','.join(map(str, ids))})")
            print("Deleted rindegastos_informes records")
        else:
            print(f"No records found in rindegastos_informes where SendDate >= {current_year}")

        conn.commit()

    except Exception as e:
        print(f"Error deleting records: {e}")

    finally:
        cursor.close()
        conn.close()

def drop_any_duplacates():
    # Drop any duplicates 
    conn = get_database_connection()
    cursor = conn.cursor()

    tables = [
        'fil.rindegastos_gastos',
        'fil.rindegastos_gastos_extrafields',
        'fil.rindegastos_gastos_sunatinfo',
        'fil.rindegastos_informes',
        'fil.rindegastos_informes_extrafields'
    ]

    try:
        for table in tables:
            cursor.execute(f'''
                WITH CTE AS (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY Id ORDER BY fecha_carga DESC) AS RowNumber
                    FROM {table}
                )
                DELETE FROM CTE
                WHERE RowNumber > 1
            ''')
            conn.commit()
            print(f"Duplicates removed from table {table}")

        print("All tables processed successfully.")

    except Exception as e:
        print(f"Error: {str(e)}")
        conn.rollback()

    finally:
        conn.close()

# API endpoint helper functions using requests
def get_expenses(params, token):
    url = "https://api.rindegastos.com/v1/getExpenses"
    headers = {"Authorization": f"Bearer {token}"}
    return requests.get(url, params=params, headers=headers, timeout=None)

def get_users(params, token):
    url = "https://api.rindegastos.com/v1/getUsers"
    headers = {"Authorization": f"Bearer {token}"}
    return requests.get(url, params=params, headers=headers, timeout=None)

def get_expense_reports(params, token):
    url = "https://api.rindegastos.com/v1/getExpenseReports"
    headers = {"Authorization": f"Bearer {token}"}
    return requests.get(url, params=params, headers=headers, timeout=None)

def get_expense_policies(params, token):
    url = "https://api.rindegastos.com/v1/getExpensePolicies"
    headers = {"Authorization": f"Bearer {token}"}
    return requests.get(url, params=params, headers=headers, timeout=None)

@log_exceptions
def main():
    if not check_api_availability():
        return
    start_time = time.time()            
    token = os.getenv('API_TOKEN')

    # Create endpoint lambdas that include the token in the API calls
    get_expenses_endpoint = lambda params: get_expenses(params, token)
    get_users_endpoint = lambda params: get_users(params, token)
    get_expense_reports_endpoint = lambda params: get_expense_reports(params, token)
    get_expense_policies_endpoint = lambda params: get_expense_policies(params, token)
    
    # Delete current year records first
    delete_rindegastos_gastos()
    delete_rindegastos_informes()
    
    # Fetch and store operations for updating current year data
    # Fetch and store expenses
    fetch_and_store_data(get_expenses_endpoint, 'rindegastos_gastos', 'Expenses', '1')
    fetch_and_store_data(get_expenses_endpoint, 'rindegastos_gastos', 'Expenses', '0')
    fetch_and_store_data(get_expenses_endpoint, 'rindegastos_gastos', 'Expenses', '2')
    
    # Fetch and store users
    fetch_and_store_data(get_users_endpoint, 'rindegastos_usuarios', 'Users')
    
    # Fetch and store expense reports
    fetch_and_store_data(get_expense_reports_endpoint, 'rindegastos_informes', 'ExpenseReports', '1')
    fetch_and_store_data(get_expense_reports_endpoint, 'rindegastos_informes', 'ExpenseReports', '0')
    
    # Fetch and store expense policies
    fetch_and_store_data(get_expense_policies_endpoint, 'rindegastos_politicas', 'Policies')
    
    # Drop any duplicates 
    drop_any_duplacates()
    
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time} seconds")

if __name__ == "__main__":
    main()
