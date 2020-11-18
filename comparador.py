import pandas as pd
import numpy as np
import requests
import datetime as dt
from sqlalchemy import create_engine
import finviz
import fmp
import keys
import time
import sqlDB
import td
import requests


def comparar_vh_vi(nombre_vh="db_vh",nombre_vi="optiones_vi"):

    sql_engine = create_engine(keys.DB_STOCKS)
    sql_conn = sql_engine.connect()

    # Descargo VI
    q = f"""SELECT ticker,VI FROM `{nombre_vi}` WHERE {nombre_vi}.ticker IN (SELECT ticker FROM `{nombre_vh}`)"""
    opcionesVI = pd.read_sql(q, con=sql_conn)
    opcionesVI.columns = ("Ticker", "VI")

    # DESCARGO VH
    q = f"""SELECT * FROM `{nombre_vh}` WHERE {nombre_vh}.ticker IN (SELECT ticker FROM `{nombre_vi}`)"""
    opcionesVH = pd.read_sql(q, con=sql_conn)

    # armo data frame completo juntando VH y VI
    resultado = opcionesVI.copy()
    resultado = pd.concat([opcionesVI,opcionesVH],axis=1)
    resultado = resultado.drop(["ticker"],axis=1)
    resultado["vh"] = resultado.vh*100
    resultado["difV"] = resultado.VI / resultado.vh -1
    resultado = resultado[["Ticker" , "VI" , "vh" , "difV" , "kurtosis" , "skew" , "lasPrice" ,
                           "0.1" , "0.2" , "0.3" , "0.4" , "0.5" , "0.6" , "0.7" , "0.8" , "0.9"]]

    resultado = resultado[resultado.VI > 0]
    resultado = resultado[resultado.vh < 1000]

    resultado = resultado[resultado.VI < resultado.vh]


    sqlDB.backup(resultado,"resultado_prueba")

    return resultado

