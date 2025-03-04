# Rindegastos

Este repositorio contiene scripts desarrollados en Python para la integración de la API de Rindegastos con las bbdd. A continuación, se describen los tres scripts principales.

## Scripts

### 1. `cargar_rindegastos.py`

Este script extrae y procesa datos de Rindegastos (gastos, usuarios, informes, políticas) mediante la API de Rindegastos y almacena esta información en las tablas correspondientes de la base de datos.

- **Funciones principales:**
  - `fetch_and_store_data`: Extrae y almacena los datos de gastos, usuarios, informes y políticas y los guarda en las tablas correspondientes. 

    Para el caso de los gastos e informes se entrega un parámetro adicional `status` que permite filtrar los gastos según su estado. Los valores posibles son:  
    - 1: Aprobado
    - 2: Rechazado
    - 0: En proceso   
    <br>
    >  Para más información consultar la [documentación oficial de la API Rindegastos](https://rindegastos.com/documentaci%C3%B3n-api).  
    <br>

  - `fetch_and_store_extrafields_data`: Extrae y almacena los campos adicionales de los gastos encontrados en la columna `extraFields`.
  - `fetch_and_store_sunatinfo_data`: Extrae y almacena la información de SUNAT asociada a los gastos encontrados en la columna `sunatInfo`.

### 2. `cargar_gastos_vcp.py`

Este script valida facturas y recibos contra la API de la SUNAT para verificar la información fiscal de los documentos.

- **Flujo:**
  1. Consulta la API de SUNAT para validar facturas y recibos.
  2. Actualiza la base de datos con la información validada.

### 3. `actualizar_informe_y_gastos_rindegastos.py`

Este script obtiene un informe específico y sus gastos relacionados desde la API de Rindegastos y actualiza la base de datos SQL Server.

- **Flujo:**

  1. Conecta a la base de datos SQL Server.
  2. Consulta el `ReportNumber` para obtener el `Id` del informe.
  3. Recupera el informe y los gastos desde la API de Rindegastos.
  4. Elimina los registros previos relacionados con el informe.
  5. Inserta los nuevos registros en las tablas:
     - `rindegastos_informes`
     - `rindegastos_informes_extrafields`
     - `rindegastos_gastos`
     - `rindegastos_gastos_extrafields`
     - `rindegastos_gastos_sunatinfo`
  6. Actualiza el estado del informe y sus gastos en la tabla `reporte_rindegastos_detalle`.

- **Manejo de errores:**

  - Implementa reintentos en las peticiones HTTP a la API.
  - Usa el decorador `@log_exceptions` para registrar errores en la base de datos.

## Requisitos

- Python 3.8+
- Librerías:
  - `requests`
  - `pandas`
  - `pyodbc`
  - `sqlalchemy`
  - `dotenv`

## Configuración

Crear un archivo `.env` con las credenciales de la base de datos y el token de la API:

```
DB_SERVER=servidor
DB_DATABASE=base_datos
DB_USERNAME=usuario
DB_PASSWORD=contraseña
API_TOKEN=token_api
```

## Ejecución

Para ejecutar `actualizar_informe_y_gastos_rindegastos.py`:

```bash
python actualizar_informe_y_gastos_rindegastos.py <report_number>
```

Sustituyendo `<report_number>` por el número del informe a actualizar.

