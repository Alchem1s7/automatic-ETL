#Importacion de las librerias necesarias
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import mysql.connector
from mysql.connector import Error
import sqlalchemy
from sqlalchemy import create_engine
from fuzzywuzzy import process
import chardet
import os

#RUTA PRINCIPAL DONDE SE ENCUENTRAN LOS ARCHIVOS CSV
ruta = input(" \n \n Introduzca la ruta donde se encuentran los archivos csv a cargar \n Hint: Click derecho sobre el folder que contiene los archivos csv  \n (Right Click > Copy path) \n ")

# :::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
# :::::::::::::::::::::::::::::::::RUTAS:::::::::::::::::::::::::::::::::
# :::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

#Creacion de una lista de strings para las los nombres de los archivos csv
lis = [file for file in os.listdir(ruta) if file.endswith(".csv")]
rute_lis = [ruta + "/" + file for file in lis]

#DETECCION DE LA CODIFICACION DE LOS ARCHIVOS CSV
new_lis = []
for ruta in rute_lis:
    with open(ruta,"rb") as x:
        data = x.read()
        y = chardet.detect(data)
        new_lis.append(y["encoding"])

#CREACION DE DATAFRAMES
dflist = [pd.read_csv(ruta, encoding=encod, on_bad_lines='skip',engine="python",sep=None) for ruta, encod in zip(rute_lis,new_lis)]

for df in dflist:
    if "Domicilio" and "X" and "Y" and "Telefono" in list(df.columns):
        clientesdf = df
    if "IdCompra" in list(df.columns) and df.shape[1] == 9:
        compradf = df
    if "IdGasto" in list(df.columns) and df.shape[1] == 5:
        gastodf = df
    if "centroide_lat" and "centroide_lon" and "localidad_censal_nombre" in list(df.columns):
        localidadesdf = df
    if "IDProveedor" and "Nombre" and "Address" and "City" in list(df.columns):
        proveedoresdf = df
    if "Sucursal" and "Direccion" and "Localidad" in list(df.columns):
        sucursalesdf = df
    if "IdVenta" in list(df.columns) and df.shape[1] == 10:
        ventadf = df
#VALORES FALTANTES ====================================================================================================

#VENTA
# Para la tabla venta vamos siempre a verificar que los valores faltantes no representen  
# una cantidad importante para asi poderlos desestimarlos de la tabla
p100_nan_precio = (ventadf.Precio.isnull().sum()*100/ventadf.shape[0])
p100_nan_cant = (ventadf.Cantidad.isnull().sum()*100/ventadf.shape[0])

if p100_nan_precio < 10:
    ventadf.dropna(subset=["Precio"],inplace=True)
else:
    ventadf.fillna(ventadf.Precio.mean(),inplace=True)

if p100_nan_cant < 10:
    ventadf.dropna(subset=["Cantidad"],inplace=True)
else:
    ventadf.fillna(ventadf.Cantidad.mode().values[0], inplace=True)
#CLIENTE
cols_null = clientesdf.isnull().sum()[clientesdf.isnull().sum() > 0].index
for column in cols_null: #Loop que elimina los null en columnas con una proporcion menor al 10% 
    if (clientesdf[column].isnull().sum())*100/(clientesdf[column].count()) < 10:
        clientesdf.dropna(subset=[column], inplace=True)
    else:
        pass

#COMPRA
cols_null = compradf.isnull().sum()[compradf.isnull().sum() > 0].index
for column in cols_null: #Loop que elimina los null en columnas con una proporcion menor al 10% 
    if (compradf[column].isnull().sum())*100/(compradf[column].count()) < 10:
        compradf.dropna(subset=[column], inplace=True)
    else:
        pass
#GASTO
cols_null = gastodf.isnull().sum()[gastodf.isnull().sum() > 0].index
for column in cols_null: #Loop que elimina los null en columnas con una proporcion menor al 10% 
    if (gastodf[column].isnull().sum())*100/(gastodf[column].count()) < 10:
        gastodf.dropna(subset=[column], inplace=True)
    else:
        pass
#LETRA CAPITAL ===============================================================================================================
capitalize = clientesdf.iloc[:,[0,1,2,3,5,6]].dtypes[clientesdf.dtypes == object].index.to_list()
for column in capitalize:
    clientesdf[column] = clientesdf[column].str.title()
#proveedoresdf
proveedoresdf.drop(columns=["Country"], inplace=True)
proveedoresdf.columns = ["Idproveedoresdf","Nombre","Domicilio","Municipio","Provincia","Departamento"]
capitalize = proveedoresdf.dtypes[proveedoresdf.dtypes == object].index.to_list()
for column in capitalize:
    proveedoresdf[column] = proveedoresdf[column].str.title()
proveedoresdf.Nombre.replace({np.nan:"Sin Dato"},inplace=True)
proveedoresdf.Provincia.replace({"Caba":"Buenos Aires"},inplace=True)

#VALORES OUTLIERS =====================================================================================================
#OUTLIERS DE PRECIO - VENTAS
q3_precio, q1_precio = np.percentile(np.array(ventadf.Precio.values), [75,25])
iqr_precio = q3_precio-q1_precio
h_outliers_precio, l_outliers_precio = q3_precio+3*iqr_precio, q1_precio-3*iqr_precio

if (ventadf[ventadf.Precio > h_outliers_precio].shape[0])*100/(ventadf.shape[0]) < 10:
    ventadf = ventadf[ventadf.Precio < h_outliers_precio] #Aqui se descartan los outliers
else:
    pass
#OUTLIERS DE CANTIDAD - VENTAS
q3_cant, q1_cant = np.percentile(np.array(ventadf.Cantidad.values), [75,25])
iqr_cant = q3_cant-q1_cant
h_outliers_cant, l_outliers_cant = q3_cant+3*iqr_cant, q1_cant-3*iqr_cant

if (ventadf[ventadf.Cantidad > h_outliers_cant].shape[0])*100/(ventadf.shape[0]) < 10:
    ventadf = ventadf[ventadf.Cantidad < h_outliers_cant] #Aqui se descartan los outliers
else:
    pass #Si los outliers superasen al 10% entonces NO se descartan y se trabajan como datos normales

#Outliers de compra
q3_precio, q1_precio = np.percentile(np.array(compradf.Precio.values), [75,25])
iqr_precio = q3_precio-q1_precio
h_outliers_precio, l_outliers_precio = q3_precio+3*iqr_precio, q1_precio-3*iqr_precio
if (compradf[compradf.Precio > h_outliers_precio].shape[0])*100/(compradf.shape[0]) < 10:
    compradf = compradf[compradf.Precio < h_outliers_precio] #Aqui se descartan los outliers
else:
    pass
#Outliers de gasto
q3_monto, q1_monto = np.percentile(np.array(gastodf.Monto.values), [75,25])
iqr = q3_monto-q1_monto
h_outliers_monto, l_outliers_monto = q3_monto+1.5*iqr, q1_monto-1.5*iqr

if (gastodf[gastodf.Monto > h_outliers_monto].shape[0])*100/(gastodf.shape[0]) < 10:
    gastodf = gastodf[gastodf.Monto < h_outliers_monto] #Aqui se descartan los outliers
else:
    pass

#DATOS INCORRECTOS
#Cliente
clientesdf.drop(columns=["col10"],inplace=True)
lat_long = clientesdf.iloc[:,7:].columns.to_list()
for column in lat_long:
    clientesdf[column] = clientesdf[column].str.replace(",",".")#Se cambian comas por puntos
    clientesdf[column] = clientesdf[column].astype(np.float16)#Se cambia el tipo de dato
    clientesdf[column] = clientesdf[column].apply(lambda x: x if x <0 else -1*x)#Se conservan solo valores (-)
#Se corrigen las coordenadas cambiadas:
clientesdf['Y_aux'] = clientesdf['Y']
clientesdf.Y[clientesdf.Y < -55] = clientesdf.X[clientesdf.Y < -55]
clientesdf.X[clientesdf.X>-55] = clientesdf.Y_aux[clientesdf.X>-55]
clientesdf.drop(columns=["Y_aux"],inplace=True)
#sucursalesdf
lat_long = sucursalesdf.iloc[:,5:].columns.to_list()
for column in lat_long:
    sucursalesdf[column] = sucursalesdf[column].str.replace(",",".")#Se cambian comas por puntos
    sucursalesdf[column] = sucursalesdf[column].astype(np.float16)#Se cambia el tipo de dato
    sucursalesdf[column] = sucursalesdf[column].apply(lambda x: x if x <0 else -1*x)#Se conservan solo valores (-)

#Se corrigen las coordenadas cambiadas:
sucursalesdf['Latitud_aux'] = sucursalesdf['Latitud']
sucursalesdf.Latitud[sucursalesdf.Latitud < -55] = sucursalesdf.Longitud[sucursalesdf.Latitud < -55]
sucursalesdf.Longitud[sucursalesdf.Longitud >-55] = sucursalesdf.Latitud_aux[sucursalesdf.Longitud>-55]
sucursalesdf.drop(columns=["Latitud_aux"],inplace=True)
#DUPLICADOS ================================================================================================
clientesdf.drop_duplicates(inplace=True)
ventadf.drop_duplicates(inplace=True)
compradf.drop_duplicates(inplace=True)
gastodf.drop_duplicates(inplace=True)

#CAMBIAR NOMBRES DE COLUMNAS ===============================================================================
#Cliente
cols_new_names = ["IdCliente","Provincia","Nombre_completo","Domicilio","Telefono", "Edad", "Localidad", "Longitud","Latitud"]
clientesdf.columns = cols_new_names
compradf.rename(columns={"Fecha_AÃ±o":"Fecha_Año"},inplace=True)
#NORMALIZACION DE STRINGS =======================================================================================
#Clientes
localidad = localidadesdf.localidad_censal_nombre.unique()#Se extraen valores unicos de ambas tablas 
bad_localidad = clientesdf.Localidad.unique()
#La siguiente funcion se encarga de normalizar mi columna de bad_localidades asignando el valor mas 
#cercano de la columna "localidad censal nombre" de la tabla maestro de clientes, devolviendome un array de localidades normalizadas
normalized = []
def get_matches(query,choices):
    for i in query:
        tuple = process.extractOne(i,choices)
        normalized.append(tuple[0])
    return normalized
good_localidad = get_matches(bad_localidad,localidad)#llamo a la funcion
map_localidades = {bad_localidad[i]:good_localidad[i] for i in range(0,len(good_localidad))} #defino un dict para usarlo con la funcion map en mi dataframe
clientesdf = clientesdf.convert_dtypes()#asigno valores adecuados a cada columna, por si las dudas
#A continuacion creo una nueva columna a partir de la vieja, mapeando y asignando nuevos valores:
clientesdf["Localidad_normalizada"] = clientesdf["Localidad"].map(map_localidades)
clientesdf = clientesdf.iloc[:,[0,1,2,3,4,5,9,6,7,8]]#cambiamos de posicion las columnas
#ahora la columna provincia
provincias = {"Ciudad de Buenos Aires":"Buenos Aires","Buenos Aires":"Buenos Aires","Córdoba":"Córdoba"}
clientesdf["Provincia_normalizada"] = clientesdf["Provincia"].map(provincias).fillna(clientesdf["Provincia"])
clientesdf = clientesdf.iloc[:,[0,10,1,2,3,4,5,6,7,8,9]]
#Dropeo y renombre de columnas ----
clientesdf.drop(columns=["Provincia","Localidad"],inplace=True)#Dropeo la columna no normalizada
clientesdf.rename(columns={"Provincia_normalizada":"Provincia","Localidad_normalizada":"Localidad"},inplace=True)#Renombro a mi columna
#sucursalesdf
sucursalesdf.Localidad.replace({"Cap. Fed.":"Cap.   Federal","Capital Federal":"Cap.   Federal","CapFed":"Cap.   Federal","Cap. Fed.":"Cap.   Federal","Capital":"Cap.   Federal","CABA":"Cap.   Federal","Cdad de Buenos Aires":"Ciudad de Buenos Aires"}, inplace=True)
#Extraigo valores malos de localidad y provincia
localidad_suc = sucursalesdf.Localidad.unique()
localidad_mstr = localidadesdf.localidad_censal_nombre.unique()
normalized = []
def get_matches(query,choices):
    for i in query:
        tuple = process.extractOne(i,choices)
        normalized.append(tuple[0])
    return normalized
good_localidad = get_matches(localidad_suc,localidad_mstr)#llamo a la funcion
map_localidadess = {localidad_suc[i]:good_localidad[i] for i in range(0,len(good_localidad))} #defino un dict para usarlo con la funcion map en mi dataframe

sucursalesdf =sucursalesdf.convert_dtypes()#asigno valores adecuados a cada columna, por si las dudas
#A continuacion creo una nueva columna a partir de la vieja, mapeando y asignando nuevos valores:
sucursalesdf["Localidad"] =sucursalesdf["Localidad"].map(map_localidadess)
#Ahora haremos lo mismo con el campo Provincia
sucursalesdf.Provincia.replace({"CABA":"Buenos Aires","Prov de Bs As.":"Buenos Aires","Pcia Bs AS":"Buenos Aires"},inplace=True)
provincia_sucursalesdf = sucursalesdf.Provincia.unique()
provincia_mstr = localidadesdf.provincia_nombre.unique()
normalized = []
def get_matches(query,choices):
    for i in query:
        tuple = process.extractOne(i,choices)
        normalized.append(tuple[0])
    return normalized
provincia_normalizada = get_matches(provincia_sucursalesdf,provincia_mstr)

map_provincias = {provincia_sucursalesdf[i]:provincia_normalizada[i] for i in range(0,len(provincia_normalizada))} #defino un dict para usarlo con la funcion map en mi dataframe
#A continuacion creo una nueva columna a partir de la vieja, mapeando y asignando nuevos valores:
sucursalesdf["Provincia"] =sucursalesdf["Provincia"].map(map_provincias)
sucursalesdf.rename(columns={"ID":"IdSucursal"},inplace=True)

#ESTABLECER LA CONEXION A LA BASE DE DATOS DE MYSQL
#Creacion de variables
host_name,db_name, u_name, u_pass, port_num  = "localhost","mydb_ind_proyect", "root", "fdlr1719", "3306"
#Conexion a la db
mydb = mysql.connector.connect(
  host=host_name,
  user=u_name,
  password=u_pass,
  database=db_name
)
print("Se ha conectado correctamente a la DB")

#Creacion del motor de base de datos
engine = create_engine("mysql+mysqlconnector://" + u_name + ":" + u_pass + "@" 
                        + host_name + ":" + port_num + "/" + db_name, echo=False)

#Importacion del dataframe a nuestra base de datos en SQL
ventadf.to_sql(name="venta", con=engine, if_exists="append", index=False)
print("Se han cargado los datos de la tabla Venta")
clientesdf.to_sql(name="cliente", con=engine, if_exists="append", index=False)
print("Se han cargado los datos de la tabla Cliente")
compradf.to_sql(name="compra", con=engine, if_exists="append", index=False)
print("Se han cargado los datos de la tabla Compra")
gastodf.to_sql(name="gasto", con=engine, if_exists="append", index=False)
print("Se han cargado los datos de la tabla Gasto")
sucursalesdf.to_sql(name="sucursalesdf", con=engine, if_exists="append", index=False)
print("Se han cargado los datos de la tabla Sucursales")
proveedoresdf.to_sql(name="proveedoresdf", con=engine, if_exists="append", index=False)
print("Se han cargado los datos de la tabla Proveedores")
print("All done.")