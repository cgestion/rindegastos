# Rindegastos

Este repositorio contiene scripts desarrollados en `python` para la integración de la `API de Rindegastos` con la base de datos `CICLO_PROVEEDORES` y esquema `fil`. A continuación, se describen los tres scripts principales.

## Scripts

### 1. `cargar_rindegastos.py`

Este script extrae y procesa datos de Rindegastos (gastos, usuarios, informes, políticas) mediante la API de Rindegastos y almacena esta información en las tablas correspondientes de la base de datos.

- **Funciones principales:**
  - `fetch_and_store_data`: Extrae y almacena los datos de gastos, usuarios, informes y políticas y los guarda en las tablas correspondientes. 

    Para el caso de los gastos e informes se entrega un parámetro adicional `status` que permite filtrar los gastos según su estado. Los valores posibles son:  
    - 1: Aprobado
    - 2: Rechazado
    - 0: En proceso<br>  
    >  Para más información consultar la [documentación oficial de la API Rindegastos](https://rindegastos.com/documentaci%C3%B3n-api).  

  - `fetch_and_store_extrafields_data`: Extrae y almacena los campos adicionales de los gastos encontrados en la columna `extraFields`.
  - `fetch_and_store_sunatinfo_data`: Extrae y almacena la información de SUNAT asociada a los gastos encontrados en la columna `sunatInfo`.

### 2. `cargar_gastos_vcp.py`

Este script valida facturas y recibos contra la API de la SUNAT para verificar la información fiscal de los documentos.

- **Flujo:**
  1. Consulta la API de SUNAT para validar facturas y recibos.
  2. Actualiza la base de datos con la información validada en la tabla `rindegastos_gastos_vcp`.

### 3. `actualizar_informe_y_gastos_rindegastos.py`

Este script se ejecuta desde Titán en la vista [`Financieros > Rendiciones > Informes rendiciones detalle`](http://titan.sayf.cl/tesoreria/reporte-rinde-gastos-detalle/index) para actualizar un informe en específico y sus gastos relacionados desde la `API de Rindegastos`.

- **Flujo:**

  1. Conecta a la base de datos.
  2. Consulta el `ReportNumber` para obtener el `Id` del informe.
  3. Recupera el informe y los gastos desde la API de Rindegastos.
  4. Elimina los registros previos relacionados con el informe.
  5. Inserta los nuevos registros en las tablas:
     - `rindegastos_informes`
     - `rindegastos_informes_extrafields`
     - `rindegastos_gastos`
     - `rindegastos_gastos_extrafields`
     - `rindegastos_gastos_sunatinfo`
  6. Actualiza el estado del informe y sus gastos en la tabla `reporte_rindegastos_detalle`, que es la que se visualiza en `Titán`.


## Ejecutar los scripts 
### 1️⃣ Clonar el repositorio
Para copiar este proyecto en tu local, abre una terminal en donde desees copiar el repo y ejecuta:

```bash
git clone https://github.com/cgestion/rindegastos.git
```

Luego, entra en la carpeta del proyecto:

```bash
cd rindegastos
```

### 2️⃣ Configurar archivo `.env`

Asegúrate de descargar y colocar el archivo `.env` disponible en la carpeta compartida de Google Drive [`DES CL - Desarrollo de negocios\Proyectos\Titán\rindegastos`](https://drive.google.com/drive/folders/1oNXjIfJAJWAtivJt7MBls7VIL_nLAfNW), en la carpeta raíz del proyecto.

> **Nota:** Google Drive a veces elimina el punto inicial del archivo `.env` al descargarlo. Asegúrate de que el archivo conserve el nombre correcto `.env`. Si ves el archivo con el nombre `env`, simplemente renómbralo y añade el punto al inicio.

**⚠️ Sin este archivo, los scripts no se ejecutarán correctamente.**

### 3️⃣ Instalar dependencias

Ejecuta el siguiente comando para instalar las dependencias necesarias:

```bash
pip install -r requirements.txt
```

### 4️⃣ Ejecutar los scripts

Para correr `cargar_rindegastos.py` y `cargar_gastos_vcp.py`, ejecuta:

```bash
python cargar_rindegastos.py
python cargar_gastos_vcp.py
```

Para ejecutar `actualizar_informe_y_gastos_rindegastos.py` se debe proporcionar el número del informe a actualizar como argumento:

```bash
python actualizar_informe_y_gastos_rindegastos.py <report_number>
```

Sustituye `<report_number>` por el número del informe a actualizar.

## 📝 Notas adicionales

Si realizas cambios en el código, no olvides ejecutar el comand `git pull` en el servidor `new-highlife` para actualizar los cambios en producción. 

La ruta del proyecto en el servidor es `C:\Compartido\Carga\Reportes\Ciclo_Proveedores\rindegastos`.  

###### Última modificación del README: 04/03/2025

---

@ Wenco-BI 2025

