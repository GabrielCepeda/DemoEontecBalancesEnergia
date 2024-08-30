import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import flatten,col,explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pandas as pd
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import os
import shutil

#Seccion de Variables

fecha_inicio="2024-06-09"
fecha_fin="2024-06-20"
id_archivo_balance_drive=""
api_key_1=""
api_key_2=""


#Inicialización de Spark
spark=spark = SparkSession.builder \
    .appName("Balances_Energia_Spark") \
    .getOrCreate()

sc = spark.sparkContext


#Descarga de los precios de la energía desde el API SIMEM
url = f"https://www.simem.co/backend-files/api/PublicData?startDate={fecha_inicio}&endDate={fecha_fin}&datasetId=96D56E"
response = requests.get(url)
dfPreciosBolsa=spark.read.json(sc.parallelize([response.text]))

#Descarga de los despachos programados de las Unidades desde el API SIMEM
url = f"https://www.simem.co/backend-files/api/PublicData?startdate={fecha_inicio}&enddate={fecha_fin}&datasetId=ff027b"
response = requests.get(url)
dfDespachosUnidades=spark.read.json(sc.parallelize([response.text]))

#Descarga del Archivo de las Unidades de generación desde la carpeta compartida en Drive de Juan
download_url = f'https://drive.google.com/uc?export=download&id={id_archivo_balance_drive}'
local_file_path = '/tmp/balances3.xlsx'
response = requests.get(download_url)

with open(local_file_path, 'wb') as file:
    file.write(response.content)

dfArchivoCapacidad = pd.read_excel(local_file_path,engine='openpyxl')
dfArchivoCapacidad.show()

#Filtrado de los atributos del precio de la bolsa
dfPreciosBolsaFiltrado=dfPreciosBolsa.select(explode(col("result.records")).alias("results")).select(col("results.*"))
dfPreciosBolsaFiltrado.show()
dfPreciosBolsaFiltrado.createOrReplaceTempView("PreciosBolsa")

#Filtrado de los atributos de los despachos de las unidades
dfResultsDespachosUnidades=dfDespachosUnidades.select(explode(col("result.records")).alias("results")).select(col("results.*"))
dfResultsDespachosUnidades.show()
dfResultsDespachosUnidades.createOrReplaceTempView("Despachos")

#Filtrado de los Despachos por las plantas correspondientes
dfDespachosAcme=spark.sql("""
                          SELECT * FROM Despachos WHERE CodigoPlanta IN ('ZPA2','ZPA3','ZPA4','ZPA5','GVIO','QUI1','CHVR') order by Valor,FechaHora DESC
                          """)

dfDespachosAcme.show()
dfDespachosAcme.createOrReplaceTempView("dfCapacidadDespachosAcme")


#Aplicación de un esquema de Spark a un dataframe de Pandas con un archivo de Excel (Mapeo)
schemaArchivo = StructType([
    StructField("useless", StringType(), True),
    StructField("fecha", StringType(), True),
    StructField("planta", StringType(), True),
    StructField("generador", StringType(), True),
    StructField("capacidad", StringType(), True),
    StructField("codigo", StringType(), True)
])
dfTransformadoCapacidad = spark.createDataFrame(dfArchivoCapacidad,schema=schemaArchivo)
dfTransformadoCapacidad.createOrReplaceTempView("CapacidadArchivo")

#Limpieza del archivo de Excel usando SQL
dfArchivoCapacidadLimpiado=spark.sql("""SELECT  b.anio,b.mes,b.dia,b.hora, b.codigo, b.capacidad  FROM 
(SELECT SUBSTR(A.FECHA, instr(A.fecha,"YEAR")+5,4) AS anio, 
regexp_replace(SUBSTR(A.FECHA, instr(A.fecha,"MONTH")+6,2),',','') AS mes,
regexp_replace(SUBSTR(A.FECHA, instr(A.fecha,"DAY_OF_MONTH")+13,2),',','') AS dia,
regexp_replace(SUBSTR(A.FECHA, instr(A.fecha,"HOUR_OF_DAY")+12,2),',','') AS hora,
* FROM CapacidadArchivo a WHERE generador IS NOT NULL AND generador != "NaN" AND generador != "GENERADOR") """)
dfArchivoCapacidadLimpiado.createOrReplaceTempView("ArchivoCapacidadLimpiado")

#Transformación de los Despachos del SIMEM para combinarlos con el archivo.
dfDespachosAcmeTransformado=spark.sql("""SELECT  day(FechaHora) as dia_despacho, month(FechaHora) as mes_despacho, year(FechaHora) as anio_despacho,hour(FechaHora) as hora_despacho, Valor as capacidad, CodigoPlanta as codigo FROM dfCapacidadDespachosAcme""")
dfDespachosAcmeTransformado.createOrReplaceTempView("DespachosAcmeTransformado")

#Balance consolidado entre el archivo de excel y los datos del API de despachos
dfBalanceConsolidado = spark.sql("""SELECT c.anio, c.mes, c.dia, c.codigo, SUM(c.balance_disponible_horario) AS consolidado_planta
FROM
(
  SELECT a.anio, a.mes, a .dia, a.hora, a.codigo, cast(a.capacidad AS DECIMAL) - cast(b.capacidad as DECIMAL) as balance_disponible_horario FROM ArchivoCapacidadLimpiado a JOIN DespachosAcmeTransformado b
  ON
  a.codigo = b. codigo AND
  a.anio = b.anio_despacho AND
  a.mes = b.mes_despacho AND
  a.dia = b.dia_despacho AND
  a.hora = b.hora_despacho
) c 
GROUP BY c.anio, c.mes, c.dia, c.codigo 
""")
dfBalanceConsolidado.createOrReplaceTempView("BalanceConsolidado")

#Aplicación de Precios de Energía al dataframe de balance consolidado
dfBalancesComprasVentasEnergia=spark.sql("""
          SELECT c.anio, c.mes, c.dia, c.codigo, c.consolidado_planta, (c.consolidado_planta * b.valor)/1000 as Compromisos_MCOP
FROM BalanceConsolidado c JOIN 
(
SELECT DAY(a.Fecha) as dia,Month(a.Fecha) as mes,Year(a.Fecha) as anio, a.Valor as valor 
FROM PreciosBolsa a 
WHERE a.CodigoVariable ="PPBOGReal" AND Version="TXR"
) b 
ON c.dia = b.dia AND
c.mes = b.mes AND
c.anio = b.anio
          """)
dfBalancesComprasVentasEnergia.createOrReplaceTempView("BalancesComprasVentasEnergia")

#Generación del reporte de balances dependiendo si es un balance negativo, se compra, o si es positivo se vende.
dfReporteCompraVentaEnergiaAcme=spark.sql("""SELECT a.*, CASE WHEN a.Compromisos_MCOP < 0 THEN "Comprar" ELSE "Vender" END as Operacion FROM BalancesComprasVentasEnergia a """)
dfReporteCompraVentaEnergiaAcme.show()

#Preparación de archivo de reporte para subir al storage
local_csv_path = "./reporteSalidaRegulador/"
dfReporteCompraVentaEnergiaAcme.coalesce(1).write.option("header", "true").mode("overwrite").csv(local_csv_path)
filepath=""
for archivo in os.listdir("/reporteSalidaRegulador/"):
 if '.csv' in archivo.name:
    filepath=archivo.path
print(filepath)
shutil.copy(filepath,"./dfReporteCompraVentaEnergiaAcme.csv")

#Autenticación con API de storage
file_upload_access_token = ""
file_upload_account_id = ""
params = {'key1': api_key_1, 'key2': api_key_2}
response=requests.get("https://fastupload.io/api/v2/authorize", params)
json_response = json.loads(response.text)
try:
  file_upload_access_token = json_response["data"]["access_token"]
  file_upload_account_id = json_response["data"]["account_id"]
except:
  print("Error autenticando y autorizando en el servicio remoto de carga de archivos.")

print(file_upload_access_token)

#Carga del archivo del reporte al storage del regulador
upload_folder_id = ""
json_response=""
with open("./dfReporteCompraVentaEnergiaAcme.csv", "rb") as archivo:
  files = {"upload_file": (archivo.name, archivo)}  # Create a dictionary for the file upload

  data = {
    "access_token": file_upload_access_token,
    "account_id": file_upload_account_id,
    "folder_id": upload_folder_id  # Include folder_id if provided
  }

  response = requests.post("https://fastupload.io/api/v2/file/upload", files=files, data=data)
  
print(response.text)