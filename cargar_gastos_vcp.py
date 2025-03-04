import requests
import json
import time
import numpy as np
from datetime import datetime
import pyodbc
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from dateutil.relativedelta import relativedelta
from params import YEARS_OFFSET, MONTHS_OFFSET, DAYS_OFFSET
import os
from cargar_rindegastos import log_exceptions

load_dotenv()

# Database connection details
server = os.getenv('DB_SERVER')
database = os.getenv('DB_DATABASE')
schema_name = os.getenv('DB_SCHEMA')
username = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')

# These dictionaries are for mapping and sending the encoded information to the API and for decoding it afterward.
# For encoding
codComp_encode = {
    "FAC": "01",  # Invoice
    "BOL": "03"   # Sales receipt
}

# For decoding
estadoCp_decode = {
    "0": "NO EXISTE",      # (Non-existent receipt),
    "1": "ACEPTADO",       # (Accepted receipt),
    "2": "ANULADO",        # (Canceled receipt),
    "3": "AUTORIZADO",     # (Authorized receipt),
    "4": "NO AUTORIZADO"   # (Not authorized by the print shop)
}

estadoRuc_decode = {
    "00": "ACTIVO",                    # Active
    "01": "BAJA PROVISIONAL",         # Provisional suspension
    "02": "BAJA PROV. POR OFICIO",    # Provisional suspension by office
    "03": "SUSPENSION TEMPORAL",      # Temporary suspension
    "10": "BAJA DEFINITIVA",          # Definitive suspension
    "11": "BAJA DE OFICIO",           # Suspension by office
    "22": "INHABILITADO-VENT.UNICA"   # Unilaterally disabled
}

condDomiRuc_decode = {
    "00": "HABIDO",           # Registered
    "09": "PENDIENTE",        # Pending
    "11": "POR VERIFICAR",    # To be verified
    "12": "NO HABIDO",        # Not registered
    "20": "NO HALLADO"        # Not found
}

# Function to consult the status
def consultar_estado(rowId, numRuc, codComp, numeroSerie, numero, fechaEmision, monto):
    # URL for integrated consultation service
    url = f"https://api.sunat.gob.pe/v1/contribuyente/contribuyentes/{numRuc}/validarcomprobante"

    # Headers of the request
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # Body of the request
    payload = {
        "numRuc": numRuc,
        "codComp": codComp_encode.get(codComp, ""),  # Access codComp_map and return empty string if not found
        "numeroSerie": numeroSerie,
        "numero": numero,
        "fechaEmision": fechaEmision,
        "monto": monto
    }

    max_retries = 3
    retries = 0

    while retries < max_retries:
        print("--------------------------------------------------------------------------------------")
        print(f"Processing rowId={rowId}, numRuc={numRuc}, codComp={codComp}, numeroSerie={numeroSerie}, numero={numero}, fechaEmision={fechaEmision}, monto={monto}")
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        if response.status_code == 200:
            # Successful request
            response_data = response.json()
            if response_data.get("data"):
                try:
                    estadoCp_text = estadoCp_decode[response_data["data"]["estadoCp"]]
                    if estadoCp_text == "NO AUTORIZADO" or estadoCp_text == "NO EXISTE":
                        print('Success')
                        return estadoCp_text, np.nan, np.nan
                    estadoRuc_text = estadoRuc_decode[response_data["data"]["estadoRuc"]]
                    condDomiRuc_text = condDomiRuc_decode[response_data["data"]["condDomiRuc"]]
                    print('Success')
                    return estadoCp_text, estadoRuc_text, condDomiRuc_text
                except KeyError as e:
                    print(response.text)
                    error_message = f"Error: Missing key in response data: {e}"
                    print(error_message)
            else:
                print(response.text)
        else:
            try:
                error_message = response.json().get("message", "Error: Sunat API could not retrieve the data")
                if error_message == "Error: Sunat API could not retrieve the data":
                    print(response.text)
                else:
                    print("API warning: ", error_message)
                if error_message == "En comprobantes físicos, el campo 'monto' no debe registrar información":
                    payload['monto'] = ''
            except TypeError:
                error_message = response.text
                print(error_message)
        print('Retrying ... ')
        time.sleep(1)
        retries += 1        

# Function to establish the database connection
def get_database_connection():
    conn_str = (
        'DRIVER={SQL Server};SERVER=' + server +
        ';DATABASE=' + database +
        ';UID=' + '{' + username + '}' +
        ';PWD=' + '{' + password + '}'
    )
    
    return pyodbc.connect(conn_str)

# Function to drop any duplicates from the target table
def drop_any_duplacates():
    # Drop any duplicates 
    conn = get_database_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f'''
            WITH CTE AS (
                SELECT 
                    Id, 
                    Fecha_Consulta,
                    ROW_NUMBER() OVER (PARTITION BY Id ORDER BY Fecha_Consulta DESC) AS rn
                FROM 
                    ciclo_proveedores.fil.rindegastos_gastos_vcp
            )
            DELETE FROM ciclo_proveedores.fil.rindegastos_gastos_vcp
            WHERE Id IN (
                SELECT Id 
                FROM CTE 
                WHERE rn > 1
            );
        ''')
        conn.commit()
        print(f"Duplicates removed from table ciclo_proveedores.fil.rindegastos_gastos_vcp")
    except Exception as e:
        print(f"Error: {str(e)}")
        conn.rollback()
    finally:
        conn.close()

@log_exceptions
def main():
    global token
    start_time = time.time()

    # Replace these values with your actual client_id and client_secret
    client_id = os.getenv('CLIENT_ID')
    client_secret = os.getenv('CLIENT_SECRET')
    ruc = os.getenv('RUC')

    # URL for token generation
    url = f"https://api-seguridad.sunat.gob.pe/v1/clientesextranet/{client_id}/oauth2/token/"

    # Body of the request
    payload = {
        "grant_type": "client_credentials",
        "scope": "https://api.sunat.gob.pe/v1/contribuyente/contribuyentes",
        "client_id": client_id,
        "client_secret": client_secret
    }

    # Making the POST request
    response = requests.post(url, data=payload)

    # Checking the response status
    if response.status_code == 200:
        # Successful request
        token = response.json().get("access_token")
    else:
        # Error occurred
        print("Error:", response.text)


    # Calculate the reference date
    reference_date = (datetime.now() - relativedelta(
        years=YEARS_OFFSET,
        months=MONTHS_OFFSET,
        days=DAYS_OFFSET,
    )).strftime("%Y-%m-%d")

    # Establish the database connection
    cnxn = pyodbc.connect(
        'DRIVER={SQL Server};SERVER=' + server + 
        ';DATABASE=' + database + 
        ';UID={' + username + '}' +
        ';PWD={' + password + '}'
    )

    # Define the SQL query to get checked_ids
    checked_ids_query = """
    SELECT Id FROM CICLO_PROVEEDORES.fil.rindegastos_gastos_vcp;
    """

    # Load checked_ids into a pandas DataFrame
    checked_ids_df = pd.read_sql_query(checked_ids_query, cnxn)

    # Extract the 'Id' column as a list or set
    checked_ids = set(checked_ids_df['Id'])

    # Define the main SQL query to load data into df
    main_query = f"""
    SELECT
        a.Id,
        RUC_Proveedor_Value,
        Tipo_Documento_Code,
        Serie_Value,
        Correlativo_Value,
        IssueDate,
        OriginalAmount
    FROM
        CICLO_PROVEEDORES.fil.rindegastos_gastos a
    LEFT JOIN
        CICLO_PROVEEDORES.fil.rindegastos_gastos_extrafields b
    ON
        a.id = b.Id
    WHERE
        a.IssueDate >= '{reference_date}';
    """

    # Load data into a pandas DataFrame
    df = pd.read_sql_query(main_query, cnxn)

    # Drop rows with any NaN values
    df.dropna(how='any', inplace=True)

    # Apply transformation
    df['Serie_Value'] = df['Serie_Value'].apply(lambda x: x.split('-')[0])

    # Filter rows where Tipo_Documento_Code is either 'FAC' or 'BOL'
    df = df[df['Tipo_Documento_Code'].isin(['FAC', 'BOL'])]

    # Drop rows from df where 'Id' is in checked_ids
    df = df[~df['Id'].isin(checked_ids)]

    # Apply consultar_estado function and create a new DataFrame
    rinde_gastos_vcp = []
    for index, row in df.iterrows():
        # Values to be passed to consultar_estado
        row_id = row['Id']
        num_ruc = row["RUC_Proveedor_Value"]
        cod_comp = row["Tipo_Documento_Code"]
        numero_serie = row["Serie_Value"]
        numero = row["Correlativo_Value"]
        fecha_emision  = datetime.strptime(row["IssueDate"], "%Y-%m-%d").strftime("%d/%m/%Y")
        monto = row["OriginalAmount"]

        result = consultar_estado(row_id,
                                  num_ruc, 
                                  cod_comp, 
                                  numero_serie, 
                                  numero, 
                                  fecha_emision, 
                                  monto)
        if result:
            rinde_gastos_vcp.append({"Id": row["Id"], 
                                     "Fecha_Consulta": datetime.now(), 
                                     "Estado_Comprobante": result[0], 
                                     "Estado_Contribuyente": result[1], 
                                     "Condicion_Domiciliaria": result[2]
                                    })

    rinde_gastos_vcp_df = pd.DataFrame(rinde_gastos_vcp)
    connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    engine = create_engine(connection_string)
    target_table = 'rindegastos_gastos_vcp'

    if not rinde_gastos_vcp_df.empty:
        rinde_gastos_vcp_df.to_sql(target_table, engine, schema=schema_name, if_exists='append', index=False)
    else:
        print("DataFrame is empty; table not replaced.")

    drop_any_duplacates()

    # Add EXEC statement at the end
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        cursor.execute(f"EXEC fil.sp_actualiza_reporte_rindegastos")  # Replace with your actual stored procedure
        conn.commit()
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time} seconds")

if __name__ == '__main__':
    main()
