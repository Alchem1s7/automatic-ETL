#Importacion de las librerias necesarias
#
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
#RUTA PRINCIPAL DONDE SE ENCUENTRAN LOS ARCHIVOS CSV
ruta = r"C:\Users\user\Desktop\daniel\DATA SCIENCE\HENRY\MATERIAL DE CLASES\DS-PI-ProyectoIndividual\Datasets/"

#RUTAS ===============================================================================================================
#Creacion de una lista de strings para las los nombres de los archivos csv
lst2 = []
lst2 = [item for item in input("Ingrese el nombre de los archivos csv en el siguiente orden, dejando un espacio entre cada palabra. \n1.CLIENTE \n2.COMPRA \n3.GASTO \n4.LOCALIDADES \n5.PROVEEDORES \n6.SUCURSALES \n7.VENTA \n : ").split()]#Con split separo los strings por "espacio"
#rutas de cada archivo completo
rutas_completas = []
for nombre_csv in lst2:
    rutas_completas.append(ruta+nombre_csv+".csv")

ruta_cliente = rutas_completas[0]
ruta_compra = rutas_completas[1]
ruta_gasto =  rutas_completas[2]
ruta_localidades =  rutas_completas[3]
ruta_proveedor =  rutas_completas[4]
ruta_sucursal =  rutas_completas[5]
ruta_venta =  rutas_completas[6]

#DETECCION DE LA CODIFICACION DE LOS ARCHIVOS CSV================================================================================================
with open(ruta_venta,"rb") as f:
    data = f.read()
    print(chardet.detect(data))
coding_venta = chardet.detect(data)

with open(ruta_cliente,"rb") as f:
    data = f.read()
    print(chardet.detect(data))
coding_cliente = chardet.detect(data)

with open(ruta_localidades,"rb") as f:
    data = f.read()
    print(chardet.detect(data))
coding_localidades = chardet.detect(data)

with open(ruta_compra,"rb") as f:
    data = f.read()
    print(chardet.detect(data))
coding_compra = chardet.detect(data)

with open(ruta_gasto,"rb") as f:
    data = f.read()
    print(chardet.detect(data))
coding_gasto = chardet.detect(data)

with open(ruta_proveedor,"rb") as f:
    data = f.read()
    print(chardet.detect(data))
coding_proveedor = chardet.detect(data)

with open(ruta_sucursal,"rb") as f:
    data = f.read()
    print(chardet.detect(data))
coding_sucursal = chardet.detect(data)


#CREACION DE DATAFRAMES ===============================================================================================
raw_venta = pd.read_csv(ruta_venta,encoding=coding_venta["encoding"])
raw_cliente = pd.read_csv(ruta_cliente,sep=";",encoding=coding_cliente["encoding"])
master_localidades = pd.read_csv(ruta_localidades,encoding=coding_localidades["encoding"])
raw_compra = pd.read_csv(ruta_compra)
rawgasto = pd.read_csv(ruta_gasto,encoding=coding_gasto["encoding"])
sucursal = pd.read_csv(ruta_sucursal,sep=";",encoding=coding_sucursal["encoding"])
proveedor = pd.read_csv(ruta_proveedor,encoding=coding_proveedor["encoding"])


#VALORES FALTANTES ====================================================================================================

#VENTA
# Para la tabla venta vamos siempre a verificar que los valores faltantes no representen  
# una cantidad importante para asi poderlos desestimarlos de la tabla
p100_nan_precio = (raw_venta.Precio.isnull().sum()*100/raw_venta.shape[0])
p100_nan_cant = (raw_venta.Cantidad.isnull().sum()*100/raw_venta.shape[0])

if p100_nan_precio < 10:
    raw_venta.dropna(subset=["Precio"],inplace=True)
else:
    raw_venta.fillna(raw_venta.Precio.mean(),inplace=True)

if p100_nan_cant < 10:
    raw_venta.dropna(subset=["Cantidad"],inplace=True)
else:
    raw_venta.fillna(raw_venta.Cantidad.mode().values[0], inplace=True)
#CLIENTE
cols_null = raw_cliente.isnull().sum()[raw_cliente.isnull().sum() > 0].index
for column in cols_null: #Loop que elimina los null en columnas con una proporcion menor al 10% 
    if (raw_cliente[column].isnull().sum())*100/(raw_cliente[column].count()) < 10:
        raw_cliente.dropna(subset=[column], inplace=True)
    else:
        pass

#COMPRA
cols_null = raw_compra.isnull().sum()[raw_compra.isnull().sum() > 0].index
for column in cols_null: #Loop que elimina los null en columnas con una proporcion menor al 10% 
    if (raw_compra[column].isnull().sum())*100/(raw_compra[column].count()) < 10:
        raw_compra.dropna(subset=[column], inplace=True)
    else:
        pass
#GASTO
cols_null = rawgasto.isnull().sum()[rawgasto.isnull().sum() > 0].index
for column in cols_null: #Loop que elimina los null en columnas con una proporcion menor al 10% 
    if (rawgasto[column].isnull().sum())*100/(rawgasto[column].count()) < 10:
        rawgasto.dropna(subset=[column], inplace=True)
    else:
        pass
#LETRA CAPITAL ===============================================================================================================
capitalize = raw_cliente.iloc[:,[0,1,2,3,5,6]].dtypes[raw_cliente.dtypes == object].index.to_list()
for column in capitalize:
    raw_cliente[column] = raw_cliente[column].str.title()
#proveedor
proveedor.drop(columns=["Country"], inplace=True)
proveedor.columns = ["IdProveedor","Nombre","Domicilio","Municipio","Provincia","Departamento"]
capitalize = proveedor.dtypes[proveedor.dtypes == object].index.to_list()
for column in capitalize:
    proveedor[column] = proveedor[column].str.title()
proveedor.Nombre.replace({np.nan:"Sin Dato"},inplace=True)
proveedor.Provincia.replace({"Caba":"Buenos Aires"},inplace=True)

#VALORES OUTLIERS =====================================================================================================
#OUTLIERS DE PRECIO - VENTAS
q3_precio, q1_precio = np.percentile(np.array(raw_venta.Precio.values), [75,25])
iqr_precio = q3_precio-q1_precio
h_outliers_precio, l_outliers_precio = q3_precio+3*iqr_precio, q1_precio-3*iqr_precio

if (raw_venta[raw_venta.Precio > h_outliers_precio].shape[0])*100/(raw_venta.shape[0]) < 10:
    raw_venta = raw_venta[raw_venta.Precio < h_outliers_precio] #Aqui se descartan los outliers
else:
    pass
#OUTLIERS DE CANTIDAD - VENTAS
q3_cant, q1_cant = np.percentile(np.array(raw_venta.Cantidad.values), [75,25])
iqr_cant = q3_cant-q1_cant
h_outliers_cant, l_outliers_cant = q3_cant+3*iqr_cant, q1_cant-3*iqr_cant

if (raw_venta[raw_venta.Cantidad > h_outliers_cant].shape[0])*100/(raw_venta.shape[0]) < 10:
    raw_venta = raw_venta[raw_venta.Cantidad < h_outliers_cant] #Aqui se descartan los outliers
else:
    pass #Si los outliers superasen al 10% entonces NO se descartan y se trabajan como datos normales

#Outliers de compra
q3_precio, q1_precio = np.percentile(np.array(raw_compra.Precio.values), [75,25])
iqr_precio = q3_precio-q1_precio
h_outliers_precio, l_outliers_precio = q3_precio+3*iqr_precio, q1_precio-3*iqr_precio
if (raw_compra[raw_compra.Precio > h_outliers_precio].shape[0])*100/(raw_compra.shape[0]) < 10:
    raw_compra = raw_compra[raw_compra.Precio < h_outliers_precio] #Aqui se descartan los outliers
else:
    pass
#Outliers de gasto
q3_monto, q1_monto = np.percentile(np.array(rawgasto.Monto.values), [75,25])
iqr = q3_monto-q1_monto
h_outliers_monto, l_outliers_monto = q3_monto+1.5*iqr, q1_monto-1.5*iqr

if (rawgasto[rawgasto.Monto > h_outliers_monto].shape[0])*100/(rawgasto.shape[0]) < 10:
    rawgasto = rawgasto[rawgasto.Monto < h_outliers_monto] #Aqui se descartan los outliers
else:
    pass

#DATOS INCORRECTOS =========================================================================================
#Cliente
raw_cliente.drop(columns=["col10"],inplace=True)
lat_long = raw_cliente.iloc[:,7:].columns.to_list()
for column in lat_long:
    raw_cliente[column] = raw_cliente[column].str.replace(",",".")#Se cambian comas por puntos
    raw_cliente[column] = raw_cliente[column].astype(np.float16)#Se cambia el tipo de dato
    raw_cliente[column] = raw_cliente[column].apply(lambda x: x if x <0 else -1*x)#Se conservan solo valores (-)
#Se corrigen las coordenadas cambiadas:
raw_cliente['Y_aux'] = raw_cliente['Y']
raw_cliente.Y[raw_cliente.Y < -55] = raw_cliente.X[raw_cliente.Y < -55]
raw_cliente.X[raw_cliente.X>-55] = raw_cliente.Y_aux[raw_cliente.X>-55]
raw_cliente.drop(columns=["Y_aux"],inplace=True)
#Sucursal
lat_long = sucursal.iloc[:,5:].columns.to_list()
for column in lat_long:
    sucursal[column] = sucursal[column].str.replace(",",".")#Se cambian comas por puntos
    sucursal[column] = sucursal[column].astype(np.float16)#Se cambia el tipo de dato
    sucursal[column] = sucursal[column].apply(lambda x: x if x <0 else -1*x)#Se conservan solo valores (-)

#Se corrigen las coordenadas cambiadas:
sucursal['Latitud_aux'] = sucursal['Latitud']
sucursal.Latitud[sucursal.Latitud < -55] = sucursal.Longitud[sucursal.Latitud < -55]
sucursal.Longitud[sucursal.Longitud >-55] = sucursal.Latitud_aux[sucursal.Longitud>-55]
sucursal.drop(columns=["Latitud_aux"],inplace=True)
#DUPLICADOS ================================================================================================
raw_cliente.drop_duplicates(inplace=True)
raw_venta.drop_duplicates(inplace=True)
raw_compra.drop_duplicates(inplace=True)
rawgasto.drop_duplicates(inplace=True)

#CAMBIAR NOMBRES DE COLUMNAS ===============================================================================
#Cliente
cols_new_names = ["IdCliente","Provincia","Nombre_completo","Domicilio","Telefono", "Edad", "Localidad", "Longitud","Latitud"]
raw_cliente.columns = cols_new_names
#NORMALIZACION DE STRINGS =======================================================================================
#Clientes
localidad = master_localidades.localidad_censal_nombre.unique()#Se extraen valores unicos de ambas tablas 
bad_localidad = raw_cliente.Localidad.unique()
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
raw_cliente = raw_cliente.convert_dtypes()#asigno valores adecuados a cada columna, por si las dudas
#A continuacion creo una nueva columna a partir de la vieja, mapeando y asignando nuevos valores:
raw_cliente["Localidad_normalizada"] = raw_cliente["Localidad"].map(map_localidades)
raw_cliente = raw_cliente.iloc[:,[0,1,2,3,4,5,9,6,7,8]]#cambiamos de posicion las columnas
#ahora la columna provincia
provincias = {"Ciudad de Buenos Aires":"Buenos Aires","Buenos Aires":"Buenos Aires","Córdoba":"Córdoba"}
raw_cliente["Provincia_normalizada"] = raw_cliente["Provincia"].map(provincias).fillna(raw_cliente["Provincia"])
raw_cliente = raw_cliente.iloc[:,[0,10,1,2,3,4,5,6,7,8,9]]
#Dropeo y renombre de columnas ----
raw_cliente.drop(columns=["Provincia","Localidad"],inplace=True)#Dropeo la columna no normalizada
raw_cliente.rename(columns={"Provincia_normalizada":"Provincia","Localidad_normalizada":"Localidad"},inplace=True)#Renombro a mi columna
#Sucursal
sucursal.Localidad.replace({"Cap. Fed.":"Cap.   Federal","Capital Federal":"Cap.   Federal","CapFed":"Cap.   Federal","Cap. Fed.":"Cap.   Federal","Capital":"Cap.   Federal","CABA":"Cap.   Federal","Cdad de Buenos Aires":"Ciudad de Buenos Aires"}, inplace=True)
#Extraigo valores malos de localidad y provincia
localidad_suc = sucursal.Localidad.unique()
localidad_mstr = master_localidades.localidad_censal_nombre.unique()
normalized = []
def get_matches(query,choices):
    for i in query:
        tuple = process.extractOne(i,choices)
        normalized.append(tuple[0])
    return normalized
good_localidad = get_matches(localidad_suc,localidad_mstr)#llamo a la funcion
map_localidadess = {localidad_suc[i]:good_localidad[i] for i in range(0,len(good_localidad))} #defino un dict para usarlo con la funcion map en mi dataframe

sucursal =sucursal.convert_dtypes()#asigno valores adecuados a cada columna, por si las dudas
#A continuacion creo una nueva columna a partir de la vieja, mapeando y asignando nuevos valores:
sucursal["Localidad"] =sucursal["Localidad"].map(map_localidadess)
#Ahora haremos lo mismo con el campo Provincia
sucursal.Provincia.replace({"CABA":"Buenos Aires","Prov de Bs As.":"Buenos Aires","Pcia Bs AS":"Buenos Aires"},inplace=True)
provincia_sucursal = sucursal.Provincia.unique()
provincia_mstr = master_localidades.provincia_nombre.unique()
print(len(provincia_sucursal),len(provincia_mstr))
normalized = []
def get_matches(query,choices):
    for i in query:
        tuple = process.extractOne(i,choices)
        normalized.append(tuple[0])
    return normalized
provincia_normalizada = get_matches(provincia_sucursal,provincia_mstr)

map_provincias = {provincia_sucursal[i]:provincia_normalizada[i] for i in range(0,len(provincia_normalizada))} #defino un dict para usarlo con la funcion map en mi dataframe
#A continuacion creo una nueva columna a partir de la vieja, mapeando y asignando nuevos valores:
sucursal["Provincia"] =sucursal["Provincia"].map(map_provincias)
sucursal.rename(columns={"ID":"IdSucursal"},inplace=True)
#CAMBIAR DE NOMBRE LAS TABLAS YA LIMPIAS ============================================================================================================
venta = raw_venta.copy()
cliente = raw_cliente.copy()
compra = raw_compra.copy()
gasto = rawgasto.copy()
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

#Creacion del motor de base de datos
engine = create_engine("mysql+mysqlconnector://" + u_name + ":" + u_pass + "@" 
                        + host_name + ":" + port_num + "/" + db_name, echo=False)
#Importacion del dataframe a nuestra base de datos en SQL
venta.to_sql(name="venta", con=engine, if_exists="append", index=False)
cliente.to_sql(name="cliente", con=engine, if_exists="append", index=False)
compra.to_sql(name="compra", con=engine, if_exists="append", index=False)
gasto.to_sql(name="gasto", con=engine, if_exists="append", index=False)
sucursal.to_sql(name="sucursal", con=engine, if_exists="append", index=False)
proveedor.to_sql(name="proveedor", con=engine, if_exists="append", index=False)

print("All done.")