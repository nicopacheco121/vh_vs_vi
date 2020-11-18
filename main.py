"""
VOLATILIDAD HISTORICA VS VOLATILIDAD IMPLICITA OPCIONES SOBRE ACCIONES

DB PAPELES
este se ejecutaria una vez por semana

#LISTO 1 Bajar tickers opcionables y hacer DB
#LISTO 2 Bajar precios historicos ajustados de acciones (yahoo o alpha) (quiza tambien agregar filtro por precio -100)
#LISTO 3 Volatilidad historica (segun el excel que ya tengo)

4 Ver spread de opciones (podría ver o papeles mas liquidos o ver por opciones mas liquidas)

DB OPCIONES
Este se ejecutaria todos los dias
#LISTO 1 bajar opciones en base a la lista de tickers de db papeles
#LISTO 2 filtrar en base a vencimietos
#LISTO 3 quitar datos que no sirvan y dejarla lo mas ordenada posible

FALTA 4 agregar la palabra
    ATM +- 2.5%
    sITM / sOTM 2.5 - 7.5
    ITM / OTM 7.5 - 25
    DITM / DOTM +25

#LISTO 5 agregar VI promedio (ya filtrados vencimientos y ATM y muy cercanos OTM e ITM)

RELACIONADOR
#LISTO 1 agregar capacidad de filtro en base a vencimientos (cierta cantidad de dias)
#LISTO 2 filtrar por las que la VI promedio de las opciones sea menor a la VH

FALTA
- Mejorar performance de descarga de db opciones
- Filtrar datos que no salieron bien o no sirven (vh y vi mayores a cierto numero o negativas)
- ver spread de opciones
- agregar ATM ITM etc (tambien podria filtrar por ultimo precio y  una % de precio para arriba o abajo)



"""

import pandas as pd
import numpy as np
import requests
import datetime as dt
from sqlalchemy import create_engine
import finviz
import keys
import time
import sqlDB
import td
import requests
import comparador
import fmp

pd.options.display.max_columns=50

"""   / / / CONEXION DB / / /   """
sql_engine = create_engine(keys.DB_STOCKS)
sql_conn = sql_engine.connect()




"""   / / / TABLA FINVIZ DE STOCKS OPCIONABLES / / /   """
#data = finviz.crearTablaFinviz(nombre="finviz",pags=range(250))
#print(data.info())




"""   / / / TABLA PRECIOS HISTORICOS / / /   """
""" Es necesario colocar por lo menos 4 o 5 keys de FMP para el armado de la base de datos
    En general con una primera corrida ya toma la mayoría de los tickers, lo que yo hice por las dudas fue verificar
    cuales NO guardo data (ticker finviz vs ticker de tabla fmp) y ejecutar la funcion 1 o 2 veces nuevamente pero
    solo con esos tickers faltantes"""


# Consulto tickers de finviz
"""q = 'SELECT Ticker FROM finviz'
tickers = pd.read_sql(q, con=sql_conn)
tickersFinviz = list(tickers["Ticker"])
"""
#print(tickersFinviz)
#print(len(tickersFinviz))


"""# Genero la tabla por primera vez
tablaPriceTickers = fmp.getInfoTotal(tercero,keys.TOKEN_FMP,nombre="poner_nombre_tabla")
print(tablaPriceTickers)
print(len(tablaPriceTickers))
"""

# Traigo solo los tickers de FMP
"""q = 'SELECT DISTINCT Ticker FROM fmp_total'
tickersFMP = pd.read_sql(q, con=sql_conn)
tickersFMP = list(tickersFMP["Ticker"])
#tickers = set(list(tickers["Ticker"]))
print(tickersFMP)
print(len(tickersFMP))"""

# Comparo con los tickers de finviz y armo una lista de los que no se subieron a la tabla
"""faltan = []
for ticker in tickersFinviz:
    if ticker not in tickersFMP:
        faltan.append(ticker)

print(faltan)
print(len(faltan))
"""



"""   / / / TABLA DE DATOS ESTADISICA / / /   """


# Hago todas las cuentas estadisticas
""" Importante en la función colocar el nombre de su DB donde guardan la data """

"""ejecutar_vh = fmp.vol_his(nombreTablaData="fmp_total",nombre="poner_nombre_tabla")
print(ejecutar_vh)"""


"""   / / / BASE DE DATOS OPCIONES / / /   """

# FILTRO ACTIVOS
precioMax = 100
fechaMax = '2018-11-03'
vhMax = 0.5
nombreTablaVH = "tabla_vh"

q = f"""SELECT * FROM `{nombreTablaVH}` WHERE \
    {nombreTablaVH}.lastPrice <= {precioMax} and \
    {nombreTablaVH}.vh <= {vhMax} and \
    {nombreTablaVH}.ticker IN \
    (SELECT Ticker FROM `finviz` WHERE `IPO_Date` <= '{fechaMax}')"""

tickersFiltrados = pd.read_sql(q,con=sql_conn)
tickersFiltrados = list(tickersFiltrados.ticker)

print(tickersFiltrados)
print(len(tickersFiltrados))

q = 'SELECT DISTINCT symbol FROM `opciones_2020-11-12`'
tickersFMP = pd.read_sql(q, con=sql_conn)
tickersFMP = list(tickersFMP["symbol"])
#tickers = set(list(tickers["Ticker"]))
print(tickersFMP)
print(len(tickersFMP))


# TABLA DE CADENA DE OPCIONES TOTAL
"""cadenasTotales = td.tablaCadenas(tickers = tickersFiltrados,nombre="asdfasdf")
print(cadenasTotales)"""



# FILTRO CONTRATOS
desde = 90
hasta = 150

#cadenaOpcionesTotal = td.optionsTotal(lista=listaTickers,desde=desde,hasta=hasta,nombre=vi_vh_otro)
#print(cadenaOpcionesTotal)




"""    / / / RESULTADO / / /   """
# PEDIR TICKERS CON MENOR VI QUE VH

#tablas = comparador.comparar_vh_vi(nombre_vh="db_vh",nombre_vi="optiones_vi")

#print(tablas)



















"""
issues
"""

"""
q = 'SELECT ticker FROM fmp'
tickers = pd.read_sql(q,con=sql_conn)
tickers = list(tickers["ticker"])
tickersFMP = list(set(tickers))
print(len(tickersFMP))


faltan= []
for ticker in tickersFinviz:
    if ticker not in tickersFMP:
        faltan.append(ticker)

print(faltan)"""



#consultar tickers de db fmp
"""q = 'SELECT Ticker FROM finviz'
tickers = pd.read_sql(q, con=sql_conn)
tickersFinviz = list(tickers["Ticker"])
print(len(tickersFinviz))
"""


#consulta de ticker especifico
"""q = "SELECT * FROM `fmp` WHERE `ticker` = 'xom' ORDER BY `ticker` DESC"
consulta = pd.read_sql(q,con=sql_conn)
print(consulta)"""

#bajar info de fmp desde una lsita de tickers
"""for t in faltan3:
    url = "https://financialmodelingprep.com/api/v3/historical-price-full/" + str(t) + "?apikey=f0bdfc9adbb9cad589dadedfd786d499"
    r = requests.get(url)
    data = r.json()
    print(data)"""