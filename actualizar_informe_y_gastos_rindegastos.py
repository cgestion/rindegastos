import sys
import pyodbc
import requests 
import pandas as pd
import json 
from sunatinfo_target_columns import sunatinfo_target_columns
import datetime
from sqlalchemy import create_engine
from cargar_rindegastos import fetch_and_store_extrafields_data, fetch_and_store_sunatinfo_data, log_exceptions
import json
from dotenv import load_dotenv
import os

load_dotenv()

# Database connection details
server = os.getenv('DB_SERVER')
database = os.getenv('DB_DATABASE')
username = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')
token = os.getenv('API_TOKEN')

# Función para establecer la conexión a la base de datos
def get_database_connection():
    conn_str = (
        'DRIVER={SQL Server};SERVER=' + server +
        ';DATABASE=' + database +
        ';UID=' + '{' + username + '}' +
        ';PWD=' + '{' + password + '}'
    )
    
    return pyodbc.connect(conn_str)
    
# Function to transform lists and dictionaries to strings
def transform_to_string(cell):
    if isinstance(cell, (list, dict)):
        return str(cell)
    else:
        return cell
        
# Custom function to convert ExtraFields
def parse_extrafields(extra_fields):
    if isinstance(extra_fields, str):
        return json.loads(extra_fields)
    return extra_fields
        
# Function to make GET requests to Rindegastos API with retry
def fetch_from_rindegastos(endpoint, params=None):
    base_url = "https://api.rindegastos.com/v1/"
    headers = {"Authorization": f"Bearer {token}"}
    url = base_url + endpoint

    for attempt in range(3):  # Make up to 3 attempts
        # print(f"Attempting to fetch data from {url}, attempt {attempt + 1}...")
        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()  # Raise an exception for HTTP errors
            # print(f"Data successfully fetched on attempt {attempt + 1}")
            return response.json()
        except requests.exceptions.RequestException as e:
            # print(f"Error on attempt {attempt + 1}: {e}")
            if attempt < 3:  # Wait 3 seconds before next attempt if not the last
                print("Retrying immediately...")
            else:
                print("Failed to fetch data after 5 attempts.")
                return None  # Return None after 5 failed attempts
                
# Función para procesar el DataFrame y cargarlo en la base de datos
def fetch_and_store_df(df, target_table, schema_name='fil', connection_string=''):    
    if target_table == 'rindegastos_gastos': 
        
        # Extract Id and ExtraFields from the dataframe for separate processing and storage
        extrafields_df = df[['Id', 'ExtraFields']].copy()
        extrafields_df['ExtraFields'] = extrafields_df['ExtraFields'].apply(parse_extrafields)
        
        # Extract Id and SunatInfo from the dataframe for separate processing and storage
        sunatinfo_df = df[['Id', 'SunatInfo']]
        
        try:
            fetch_and_store_extrafields_data(extrafields_df, 'rindegastos_gastos_extrafields', "0")
        except Exception as e:
            print(f"Error in fetch_and_store_extrafields_data: {e}")
        try:
            fetch_and_store_sunatinfo_data(sunatinfo_df, 'rindegastos_gastos_sunatinfo', "0")
        except Exception as e:
            print(f"Error in fetch_and_store_sunatinfo_data: {e}")
            
    else:
        # Extract Id and ExtraFields from the dataframe for separate processing and storage
        extrafields_df = df[['Id', 'ExtraFields']].copy()
        extrafields_df['ExtraFields'] = extrafields_df['ExtraFields'].apply(parse_extrafields)
        
        try:
            fetch_and_store_extrafields_data(extrafields_df, 'rindegastos_informes_extrafields', '0')
        except Exception as e:
            print(f"Error in fetch_and_store_extrafields_data: {e}")

    engine = create_engine(connection_string)
    
    # Aplicar la transformación a cada elemento del DataFrame
    df = df.map(transform_to_string)
    
    df.drop_duplicates(inplace=True)
    
    df['fecha_carga'] = datetime.datetime.now().strftime("%d-%m-%Y %H:%M")     
    
    df.to_sql(target_table, engine, schema=schema_name, if_exists='append', index=False)
    
@log_exceptions
def main(report_number):
    # Connect to the database
    conn = get_database_connection()
    cursor = conn.cursor()
    
    try:
        # Step 1: Fetch ID from rindegastos_informes where ReportNumber is equal to report_number
        cursor.execute(f"SELECT TOP 1 Id FROM fil.rindegastos_informes WHERE ReportNumber={report_number}")
        report_id = cursor.fetchone()[0]
        
    except Exception as e:
        print(f"An error occurred while fetching IDs: {e}")
    
    # Use fetch_from_rindegastos to get the expense report data
    report_data = fetch_from_rindegastos("getExpenseReport", params={"Id": report_id})
    if report_data is None:
        raise Exception("Failed to fetch expense report data")
        
    # Create the Report DataFrame
    report_df = pd.DataFrame([report_data])

    # Use fetch_from_rindegastos to get expenses related to the report
    expenses_data = fetch_from_rindegastos("getExpenses", params={"ReportId": report_id})
    if expenses_data is None:
        raise Exception("Failed to fetch expenses data")

    # Extract expenses records and create the Expenses DataFrame
    records = expenses_data.get('Expenses', [])
    expenses_df = pd.DataFrame(records)

    # Expenses ID list. Aquí nos aseguramos de eliminar todo lo relacionado al informe. Incluyendo aquello gastos que pudieron ser elimnados 
    try:
        # Step 1: Fetch Id from rindegastos_informes where ReportNumber is equal to report_number
        cursor.execute(f"SELECT Id FROM ciclo_proveedores.fil.rindegastos_gastos WHERE ReportId={report_id}")
        existing_expense_ids = [row[0] for row in cursor.fetchall()]
        
    except Exception as e:
        print(f"An error occurred while fetching existing_expense_ids: {e}")

    try:
        # Delete records from the database
        cursor.execute(f"DELETE FROM ciclo_proveedores.fil.rindegastos_informes_extrafields WHERE Id ={report_id}")
        cursor.execute(f"DELETE FROM ciclo_proveedores.fil.rindegastos_informes WHERE Id ={report_id}")
        cursor.execute(f"DELETE FROM ciclo_proveedores.fil.rindegastos_gastos_extrafields WHERE Id IN ({','.join(map(str, existing_expense_ids))})")
        cursor.execute(f"DELETE FROM ciclo_proveedores.fil.rindegastos_gastos_sunatinfo WHERE Id IN ({','.join(map(str, existing_expense_ids))})")
        cursor.execute(f"DELETE FROM ciclo_proveedores.fil.rindegastos_gastos WHERE Id IN ({','.join(map(str, existing_expense_ids))})")
        conn.commit()  # Commit the transaction
        
    except Exception as e:
        print(f"Error deleting records: {e}")
        conn.rollback()  # Rollback the transaction in case of error

    finally:
        # Close connection
        cursor.close()
        conn.close()

    # Connection string
    connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"

    # Procesar expenses_df
    fetch_and_store_df(expenses_df, 'rindegastos_gastos', connection_string=connection_string)

    # Procesar report_df con lógica especial
    fetch_and_store_df(report_df, 'rindegastos_informes', connection_string=connection_string)

    # Trasnformamos el dataframe en fila para obtener sus columnas con mayor facilidad
    report_data =  report_df.iloc[0]

    # Definimos los valores de las columnas que serán comunes para todos los gastos presentes en el informe
    Aprobador = report_data.ApproverName
    Informe_Estado = 'En Proceso' if report_data.Status == 0 else 'Cerrado' if report_data.Status == 1 else None
    Informe_Estado_Interno = 'Contabilizado' if report_data.CustomStatus.strip() == 'Contabilizado' else 'No Contabilizado'

    # Definimos un dataframe con los valores de las columnas que deben ser actualizadas para los gastos en ciclo_proveedores.fil.reporte_rindegastos_detalle
    conn = get_database_connection()
    cursor = conn.cursor()

    try:
        # Step 1: Fetch expense data that will be updated in table 
        cursor.execute(f"""
        SELECT 
            rg.Id as ExpenseId,
            rg.IssueDate AS Gasto_Fecha,
            Serie_Value,
            Correlativo_Value,
            Category as Gasto_Categoria,
            CategoryCode as Gasto_Cuenta_id, 
            Centro_Costo_Code as Centro_costo_code,
            Tipo_Documento_Value,
            RUC_Proveedor_Value,
            Supplier as Gasto_Proveedor,
            Impuesto_Code,
            case 
                when rg.Status = 1 then 'Aprobado'
                when rg.Status = 2 then 'Rechazado'
                when rg.Status = 0 then 'En Proceso' 
            end as Gasto_Estado,
            Net as Gasto_Monto_neto, 
            Tax as Gasto_Impuesto, 
            OtherTaxes as Gasto_Otros_impuestos,
            Total as Gasto_Monto_Total
        FROM 
            fil.rindegastos_gastos rg
        LEFT JOIN 
            fil.rindegastos_gastos_extrafields rge
        ON 
            rg.Id = rge.Id
        WHERE 
            rg.ReportId IN (
                SELECT Id 
                FROM fil.rindegastos_informes 
                WHERE ReportNumber = {report_number}
            );
        """)
        # Initialize an empty list to store dictionaries (rows)
        rows = []
        
        # Fetch rows one by one and append as dictionaries
        for row in cursor:
            row_dict = {
                'ExpenseId': row[0],
                'Gasto_Fecha': row[1],
                'Serie_Value': row[2],
                'Correlativo_Value': row[3],
                'Gasto_Categoria': row[4],
                'Gasto_Cuenta_id': row[5],
                'Centro_costo_code': row[6],
                'Tipo_Documento_Value': row[7],
                'RUC_Proveedor_Value': row[8],
                'Gasto_Proveedor': row[9],
                'Impuesto_Code': row[10],
                'Gasto_Estado': row[11],
                'Gasto_Monto_neto': row[12],
                'Gasto_Impuesto': row[13],
                'Gasto_Otros_impuestos': row[14],
                'Gasto_Monto_Total': row[15]
            }
            rows.append(row_dict)
        
        # Convert list of dictionaries to DataFrame
        cols_to_update_df = pd.DataFrame(rows)
        
    except Exception as e:
        print(f"An error occurred while fetching the expenses: {e}")

    new_expense_ids = expenses_df['Id'].tolist() # Esta data viene directo de la API. 

    # Convert to sets for easier comparison
    existing_expense_ids_set = set(existing_expense_ids) # Existing expense ids viene de rindegastos_gastos (antes del DELETE)
    new_expense_ids_set = set(new_expense_ids)

    # Find ExpenseIds that are in existing but not in new
    expense_ids_to_delete = existing_expense_ids_set - new_expense_ids_set

    # Esto es para obtener los ExpenseIds que existían previamente, para luego compararlos con los ExpenseIds del dataframe update_values.
    # Si hay un ExpenseId que no se encuentre en los new_expense_ids debemos hacer DELETE FROM ciclo_proveedores.fil.reporte_rindegastos_detalle WHERE ExpenseId =   
    try:
        # Perform deletion for each ExpenseId that is no longer present
        if expense_ids_to_delete:
            delete_query = "DELETE FROM ciclo_proveedores.fil.reporte_rindegastos_detalle WHERE ExpenseId = ?"
            conn = get_database_connection()
            cursor = conn.cursor()
            for expense_id in expense_ids_to_delete:
                cursor.execute(delete_query, expense_id)
                print(f"Deleted ExpenseId {expense_id}")
            conn.commit()
        
        # Perform update for each ExpenseId present in cols_to_update_df
        update_query = """UPDATE ciclo_proveedores.fil.reporte_rindegastos_detalle 
                        SET Gasto_Fecha = ?, Serie_Value = ?, Correlativo_Value = ?, Gasto_Categoria = ?, Gasto_Cuenta_id = ?, Centro_costo_code = ?, Tipo_Documento_Value = ?, RUC_Proveedor_Value = ?, Gasto_Proveedor = ?, Impuesto_Code = ?, Gasto_Estado = ?, Gasto_Monto_neto = ?, Gasto_Impuesto = ?, Gasto_Otros_impuestos = ?, Gasto_Monto_Total = ?, Aprobador = ?, Informe_Estado = ?
                        WHERE ExpenseId = ?"""
        conn = get_database_connection()
        cursor = conn.cursor()

        Gasto_Monto_neto = 0
        Gasto_Impuesto = 0
        Gasto_Monto_Total = 0

        for index, row in cols_to_update_df.iterrows():
            Gasto_Monto_neto += row['Gasto_Monto_neto']
            Gasto_Impuesto += row['Gasto_Impuesto']
            Gasto_Monto_Total += row['Gasto_Monto_Total']
            cursor.execute(update_query, row['Gasto_Fecha'], row['Serie_Value'], row['Correlativo_Value'], row['Gasto_Categoria'], row['Gasto_Cuenta_id'], row['Centro_costo_code'], row['Tipo_Documento_Value'], row['RUC_Proveedor_Value'], row['Gasto_Proveedor'], row['Impuesto_Code'], row['Gasto_Estado'], row['Gasto_Monto_neto'], row['Gasto_Impuesto'], row['Gasto_Otros_impuestos'], row['Gasto_Monto_Total'], Aprobador, Informe_Estado, row['ExpenseId'])
            print(f"Updated ExpenseId {row['ExpenseId']}")

        update_query_resumen = """UPDATE ciclo_proveedores.fil.reporte_rindegastos_resumen 
                                SET Aprobador = ?, Informe_Estado = ?, Informe_Estado_Interno = ?, Gasto_Monto_neto = ?, Gasto_Impuesto = ?, Gasto_Monto_Total = ?
                                WHERE Informe_ID = ?"""
        cursor.execute(update_query_resumen, Aprobador, Informe_Estado, Informe_Estado_Interno, Gasto_Monto_neto, Gasto_Impuesto, Gasto_Monto_Total, report_number)
        conn.commit()
        print(f"Updated Report with Informe_ID {report_number}")

    except Exception as e:
        print(f"An error occurred: {e}")
    
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python actualizar_reporte_y_gastos_asociados.py <report_number>")
        sys.exit(1)

    report_number = int(sys.argv[1])
    main(report_number)