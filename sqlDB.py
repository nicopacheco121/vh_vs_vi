from sqlalchemy import create_engine
import keys
import pandas as pd


def connection():

    # Seteo el USER : PASS @ HOST / BBDD_NAME
    sql_engine = create_engine(keys.DB_STOCKS)
    conectado = sql_engine.connect()

    return conectado


def backup(df,nombre,if_exists="replace"):

    sql_conn = connection()
    df.to_sql(con=sql_conn, name=nombre, if_exists=if_exists)

    return "guardado en la base de datos"


def tabla_finviz(nombre):

    sql_conn = connection()

    create_table = f"""
    CREATE TABLE IF NOT EXISTS `{nombre}` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `ticker` varchar(20) DEFAULT '',
    `company` varchar(60) DEFAULT '',
    `sector` varchar(60) DEFAULT '',
    `industry` varchar(60) DEFAULT '',
    `country` varchar(30) DEFAULT '',
    `market_cap` float NULL DEFAULT NULL,
    `price` float(10) DEFAULT NULL,
    `ipo_date` datetime DEFAULT NULL,
    PRIMARY KEY (`id`) )"""

    sql_conn.execute(create_table)
    return "Tabla creada correctamente"

def tabla_fmp(nombre="fmp_prueba"):

    sql_conn = connection()

    create_table = f"""
    CREATE TABLE IF NOT EXISTS `{nombre}` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `date` datetime DEFAULT NULL,
    `open` float(10) DEFAULT NULL,
    `high` float(10) DEFAULT NULL,
    `low` float(10) DEFAULT NULL,
    `close` float(10) DEFAULT NULL,
    `adjClose` float(10) DEFAULT NULL,
    `volume` int(20) DEFAULT NULL,
    `change` float(10) DEFAULT NULL,
    `vh75` float(10) DEFAULT NULL,
    `ticker` varchar(20) DEFAULT '',
    PRIMARY KEY (`id`) )"""

    sql_conn.execute(create_table)
    return "Tabla creada correctamente"

def tabla_vh(nombre="tabla_vh"):

    sql_conn = connection()

    create_table = f"""
    CREATE TABLE IF NOT EXISTS `{nombre}` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `ticker` varchar(20) DEFAULT '',
    `lastPrice` float(10) DEFAULT NULL,
    `vh` float(10) DEFAULT NULL,
    `kurtosis` float(10) DEFAULT NULL,
    `skew` float(10) DEFAULT NULL,
    `0.1` float(10) DEFAULT NULL,
    `0.2` float(10) DEFAULT NULL,
    `0.3` float(10) DEFAULT NULL,
    `0.4` float(10) DEFAULT NULL,
    `0.5` float(10) DEFAULT NULL,
    `0.6` float(10) DEFAULT NULL,
    `0.7` float(10) DEFAULT NULL,
    `0.8` float(10) DEFAULT NULL,
    `0.9` float(10) DEFAULT NULL,
    PRIMARY KEY (`id`) )"""

    sql_conn.execute(create_table)
    return "Tabla creada correctamente"

def tabla_cadenaOpciones(nombre):
    sql_conn = connection()

    create_table = f"""
        CREATE TABLE IF NOT EXISTS `{nombre}` (
        `id` int(11) NOT NULL AUTO_INCREMENT,
        `symbol_opc` varchar(20) DEFAULT '',
        `strike` float(10) DEFAULT NULL,
        `TTM` int(10) DEFAULT NULL,
        `type` float(10) DEFAULT NULL,
        `bid` float(10) DEFAULT NULL,
        `ask` float(10) DEFAULT NULL,
        `last` float(10) DEFAULT NULL,
        `IV` float(10) DEFAULT NULL,
        `openint` float(10) DEFAULT NULL,
        `theor` float(10) DEFAULT NULL,
        `delta` float(10) DEFAULT NULL,
        `gamma` float(10) DEFAULT NULL,
        `theta` float(10) DEFAULT NULL,
        `vega` float(10) DEFAULT NULL,
        `rho` float(10) DEFAULT NULL,
        `ITM` varchar(20) DEFAULT NULL,
        `symbol` varchar(20) DEFAULT NULL,
        PRIMARY KEY (`id`) )"""

    sql_conn.execute(create_table)
    return "Tabla creada correctamente"

def tabla_alpaca(nombre):
    sql_conn = connection()

    create_table = f"""
        CREATE TABLE IF NOT EXISTS `{nombre}` (
        `id` int(11) NOT NULL AUTO_INCREMENT,
        `symbol` varchar(20) DEFAULT '',
        `price` float(10) DEFAULT NULL,
        PRIMARY KEY (`id`) )"""

    sql_conn.execute(create_table)
    return "Tabla creada correctamente"


def consulta_tickers(tabla):
    sql_engine = create_engine(keys.DB_STOCKS)
    sql_conn = sql_engine.connect()

    q = f'SELECT Ticker FROM {tabla}'
    resultado = pd.read_sql(q, con=sql_conn)
    resultado = list(resultado["Ticker"])

    return resultado

def consulta_tickers_unicos(tabla):
    sql_engine = create_engine(keys.DB_STOCKS)
    sql_conn = sql_engine.connect()

    q = f'SELECT DISTINCT Ticker FROM {tabla}'
    resultado = pd.read_sql(q, con=sql_conn)
    resultado = list(resultado["Ticker"])

    return resultado

