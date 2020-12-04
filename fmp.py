import pandas as pd
import numpy as np
import requests
import datetime as dt
from sqlalchemy import create_engine
import finviz
import keys
import sqlDB
import tqdm, sys
import time
import logging

def listaStringTickers(listaTickers,cantidad=5):
    """ Recibe un listado de tickers y entrega un lista de listas con tickers en string segun la cantidad colocada"""

    listas = []
    for i in range(len(listaTickers)//cantidad +1):
        listas.append(listaTickers [i*cantidad : (i+1)*cantidad])

    for i in range(len(listas)):
        listas[i] = ','.join(listas[i])

    return listas

def defKey(listaKeys,ubicacion):
    """ Recibe una lista de key y las va cambiando cada una cierta cantidad """
    import time

    ubicacion= ubicacion+1
    if ubicacion == len(listaKeys):
        ubicacion = 0
    key = listaKeys[ubicacion]

    time.sleep(30)

    return key,ubicacion


def getInfoTotal(listaTickers,listaKeys,nombre="fmp"):

    """
    Recibe un listado de tickers y sube a la base de datos el historico de 5 años, adiciona change y volatilidad
    de 75 ruedas

    """
    start_time = time.time()

    # creo la tabla por primera vez
    sqlDB.tabla_fmp(nombre=nombre)

    # paso a string las listas de tickers
    listaTickersString = listaStringTickers(listaTickers)

    noEncontrados = []
    contador = -1
    dictKey = {"key": listaKeys[0], "ubicacionKey": 0}

    with tqdm.tqdm(total=len(listaTickersString), file=sys.stdout) as pbar:
        for t in listaTickersString:

            pbar.update()
            contador+=1

            time.sleep(1)

            # Verifico si es necesario cambiar la key
            if (contador % 240 == 0) and contador !=0:
                tuplaKey = defKey(listaKeys=listaKeys,ubicacion=dictKey["ubicacionKey"])
                dictKey["key"] = tuplaKey[0]
                dictKey["ubicacionKey"]= tuplaKey[1]
                print(dictKey["key"])

            continua = False

            # Pruebo bajar la info total y si da error me avisa
            try:
                # Descargo la info de a 5 tickers
                url = "https://financialmodelingprep.com/api/v3/historical-price-full/"+str(t)+"?apikey="+str(dictKey["key"])
                r = requests.get(url)
                data = r.json()

                # Si no encuentra ningun valor devuelve un json vacio
                if data != {}:
                    continua = True
                else:
                    noEncontrados.append(t)

            except:
                print(f"dio error{t}")
                noEncontrados.append(t)
                continua = False
                pass

            # Hago los procesos y  guardo en la base de datos
            if continua:
                tickersEnlista = t.split(sep=",")
                encontrados = []

                if len(tickersEnlista) != 1:
                    data_total = data['historicalStockList']
                else:
                    noEncontrados.append(tickersEnlista[0])
                    break

                for i in range(len(data_total)):

                    try:
                        ticker = data_total[i]['symbol']
                        datos = data_total[i]['historical']

                        df = pd.DataFrame(datos)
                        df = df.drop(['unadjustedVolume', 'change', 'changePercent', 'vwap', 'label', 'changeOverTime'],
                                     axis=1)
                        df = finviz.aFlotante(df=df, columnas=df.columns)
                        df["date"] = pd.to_datetime(df["date"])
                        df = df.sort_values("date", ascending=True)

                        df["change"] = df.adjClose.pct_change()
                        df["vh75"] = (df.change.rolling(75).std()) * (252 ** 0.5)
                        df["ticker"] = ticker
                        df = df.dropna()
                        df = df.set_index("date")

                        sqlDB.backup(df, nombre=nombre, if_exists='append')

                        encontrados.append(ticker)

                    except:
                        print(f"hubo otro error con {ticker}")
                        noEncontrados.append(ticker)
                        pass

                for x in tickersEnlista:
                    if x not in encontrados and x not in noEncontrados:
                        noEncontrados.append(x)

    print("--- %s seconds ---" % (time.time() - start_time))

    return noEncontrados

def vol_his(nombreTablaData, nombre="tabla_vh"):
    start_time = time.time()

    logging.basicConfig(level=logging.INFO, format='{asctime} {levelname} ({threadName:11s}) {message}', style='{')

    logging.info(f"iniciando")

    # conectandose a la DB
    sql_engine = create_engine(keys.DB_STOCKS)
    sql_conn = sql_engine.connect()

    # creo la tabla por primera vez
    sqlDB.tabla_vh(nombre=nombre)

    lista_errores = []
    lista = []

    # levanto la base de datos
    q = f"SELECT date,adjClose,vh75,ticker FROM `{nombreTablaData}` ORDER BY `date` ASC"

    logging.info(f"bajando la tabla de datos totales")
    start_time_tabla = time.time()
    totalFMP = pd.read_sql(q, con=sql_conn)
    logging.info(f"baje la tabla")
    print("--- %s seconds ---" % (time.time() - start_time_tabla))

    # pido solo los tickers
    tickers = list(set(list(totalFMP["ticker"])))
    print(tickers)
    print(len(tickers))

    contador = -1
    for t in tickers:

        contador += 1
        print(contador)

        try:

            data = totalFMP[totalFMP.ticker == t]

            # armo el df con ticker, vh, kurtosis, skew y deciles
            dic = {}
            dic["ticker"] = t
            dic["lastPrice"] = data.adjClose.iloc[-1]
            dic["vh"] = data.vh75.mean()
            dic["kurtosis"] = data.vh75.kurtosis()
            dic["skew"] = data.vh75.skew()
            dic["0.1"] = data.vh75.quantile(q=0.1)
            dic["0.2"] = data.vh75.quantile(q=0.2)
            dic["0.3"] = data.vh75.quantile(q=0.3)
            dic["0.4"] = data.vh75.quantile(q=0.4)
            dic["0.5"] = data.vh75.quantile(q=0.5)
            dic["0.6"] = data.vh75.quantile(q=0.6)
            dic["0.7"] = data.vh75.quantile(q=0.7)
            dic["0.8"] = data.vh75.quantile(q=0.8)
            dic["0.9"] = data.vh75.quantile(q=0.9)

            lista.append(dic)

        except:
            lista_errores.append(t)

    lista = pd.DataFrame(lista)
    lista = lista.set_index("ticker")
    print(lista)
    sqlDB.backup(lista, nombre=nombre, if_exists='append')

    print("--- %s seconds ---" % (time.time() - start_time))

    return lista_errores


""" ALPACA - PRECIO DIARIO """
def bajada_alpaca(symbol,public,secret,timeframe='1D',limit=1):

    url = f'https://data.alpaca.markets/v1/bars/{timeframe}'
    headers = {'APCA-API-KEY-ID': public, 'APCA-API-SECRET-KEY': secret}
    params = {'symbols' : symbol, 'limit': limit}

    r = requests.get(url=url,headers=headers , params=params)
    js = r.json()

    return js

def listaActivos(lista):
    """ Recibe una lista de activos y la separa en varias listas de a 200 tickers """
    listas = []
    for i in range (len(lista)//200+1):
        listas.append(lista[i*200 : (i+1)*200])

    return listas

def sublist_to_string(listas):
    """ Recibe un listado de listas de tickers y por cada lista hace un string con los ticker separados por comas """

    res = []
    for i in range(len(listas)):
        res.append(','.join(listas[i]))
    return res

def worker_alpaca(tickers,nombre,public,secret):

    precios = bajada_alpaca(tickers[0], public=public, secret=secret)  # bajo los precios
    tickers_en_lista = tickers[0].split(sep=",")
    lista_tickers = []
    lista_precios = []

    for ticker in tickers_en_lista:
        print(ticker, end=' ')

        try:
            precio = precios[ticker][0]['c']
            lista_tickers.append(ticker)
            lista_precios.append(precio)
        except:
            continue
    para_df = {'ticker' : lista_tickers, 'price': lista_precios}
    # Armo el df

    df = pd.DataFrame(para_df)
    df.set_index('ticker',inplace=True)
    #df.columns = ['symbol','price']

    if len(df) == 0:
        print('  / / / / // / ')
    else:
        sqlDB.backup(df=df, nombre=nombre, if_exists="append")

    return df


def precio_alpaca(tickers,nombre,public,secret):
    import threading

    start_time = time.time()

    # Creo la tabla por primera vez
    import datetime as dt
    hoy = str(dt.date.today())
    nombre = nombre+hoy
    sqlDB.tabla_alpaca(nombre=nombre)

    # Seteo la lista de tickers para que quede en una sola lista, de a 200 tickers
    tickers = listaActivos(tickers)
    tickers = sublist_to_string(tickers)

    # descargo la info y la voy subiendo a la db
    noEncontrados = []
    n_threads = len(tickers)
    subs = np.array_split(tickers, n_threads) # separo en cantidad de listas segun cantidad de threads

    # hilos
    threads = []
    for i in range(n_threads):
        t = threading.Thread(target=worker_alpaca, args=((subs[i]),nombre,public,secret))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()


    print("--- %s seconds ---" % (time.time() - start_time))

    return noEncontrados




""" CODIGO DE ALPHAVANTIGE - NO SE USA   """
def defKeyAlpha(listaKeys,ubicacion):
    """ Recibe una lista de key y las va cambiando cada una cierta cantidad """
    import time

    ubicacion= ubicacion+1
    if ubicacion == len(listaKeys):
        ubicacion = 0
    key = listaKeys[ubicacion]

    time.sleep(30)

    return key,ubicacion


def getInfoTotalAlpha(listaTickers,listaKeys,nombre="alpha"):
    """
    Inputs
    ------
    Lista de tickers a descargar (ultimos 10 años) y una lista de api key

    Returns
    ------
    Guarda en la DB la informacion historica de precios
    Devuelve una lista con los tickers que fallaron
    """
    noEncontrados = []

    contador = -1
    dictKey = {"key": listaKeys[0], "ubicacionKey": 0}
    with tqdm.tqdm(total=len(listaTickers), file=sys.stdout) as pbar:
        for t in listaTickers:
            pbar.update()
            contador+=1

            try:
                if (contador % 5 == 0) and contador !=0:
                    tuplaKey = defKey(listaKeys=listaKeys,ubicacion=dictKey["ubicacionKey"])
                    dictKey["key"] = tuplaKey[0]
                    dictKey["ubicacionKey"]= tuplaKey[1]
                    print(dictKey["key"])

                # Descargo la info ticker a ticker
                url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED"
                params = {"symbol":t,"outputsize":"full","apikey":dictKey["key"]}
                datos = requests.get(url,params=params)
                datos = datos.json()

                # Trato la info
                datos = datos["Time Series (Daily)"]
                datos = pd.DataFrame(datos)
                datos = datos.T

                datos = datos.iloc[0:3650]
                datos = datos.reset_index().sort_values("index",ascending=True)
                datos.columns = ["date","open","high","low","close","adjusted_close","volume",
                                 "divident_amount","split_coefficient"]

                datos = finviz.aFlotante(datos,datos.columns)

                datos["Change"] = datos.adjusted_close.pct_change()
                datos["Ticker"] = t

                # Back up
                sqlDB.backup(datos, nombre=nombre,if_exists='append')

            except:
                noEncontrados.append(t)

        return noEncontrados