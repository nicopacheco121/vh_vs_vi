import requests
import pandas as pd
import keys
import numpy as np
import sqlDB
import time
import threading

def options(ticker,key=keys.TOKEN_TD):
    endpoint = "https://api.tdameritrade.com/v1/marketdata/chains"
    params = {'apikey': key, 'symbol': ticker}
    r = requests.get(url=endpoint, params=params)
    return r.json()


def optionsTotal(lista,desde=None,hasta=None,nombre="optiones_vi"):
    start_time = time.time()
    diccionario = {}
    tickers = []
    vi = []
    noEncontrados = []
    for ticker in lista:

        try:
            # descargar información
            chain = options(ticker=ticker)

            # listo fechas por un lado y valores por otro de call y put
            v_calls = list(chain['callExpDateMap'].values())
            v_puts = list(chain['putExpDateMap'].values())

            # armo lista opciones
            callsVI = []
            for i in range(len(v_calls)):
                v = list(v_calls[i].values())
                for j in range(len(v)):
                    diasFaltan = v[j][0]['daysToExpiration']
                    viC = float(v[j][0]['volatility'])

                    # filtro por los días que faltan y que no sea nulo el VI
                    if diasFaltan >= desde and diasFaltan <= hasta and np.isnan(viC) == False:
                        callsVI.append(viC)

            putsVI = []
            for i in range(len(v_puts)):
                v = list(v_puts[i].values())
                for j in range(len(v)):
                    diasFaltan = v[j][0]['daysToExpiration']
                    viP = float(v[j][0]['volatility'])
                    if diasFaltan >= desde and diasFaltan <= hasta and np.isnan(viP) == False:
                        putsVI.append(viP)

            # promedio VI
            promedioVI = (sum(callsVI) + sum(putsVI)) / (len(callsVI) + len(putsVI))
            tickers.append(ticker)
            vi.append(promedioVI)
        except:
            noEncontrados.append(ticker)

    diccionario["ticker"] = tickers
    diccionario["VI"] = vi

    r = pd.DataFrame(diccionario)

    # back up
    sqlDB.backup(df=r,nombre=nombre,if_exists="replace")
    print("--- %s seconds ---" % (time.time() - start_time))

    return noEncontrados

def optionsDF(chain):
    try:
        v_calls = list(chain['callExpDateMap'].values())
        v_calls_fechas = list(chain['callExpDateMap'].keys())
        v_puts = list(chain['putExpDateMap'].values())
        v_puts_fechas = list(chain['putExpDateMap'].keys())
        calls = []
        for i in range(len(v_calls)):
            v = list(v_calls[i].values())
            for j in range(len(v)):
                calls.append(v[j][0])

        puts = []
        for i in range(len(v_puts)):
            v = list(v_puts[i].values())
            for j in range(len(v)):
                puts.append(v[j][0])

        contracts = pd.concat([pd.DataFrame(calls), pd.DataFrame(puts)])
        tabla = contracts.loc[contracts.daysToExpiration > 0]
        tabla = tabla.loc[:, ['symbol', 'strikePrice', 'daysToExpiration', 'putCall', 'bid', 'ask',
                              'last', 'volatility', 'openInterest', 'theoreticalOptionValue',
                              'delta', 'gamma', 'theta', 'vega', 'rho', 'inTheMoney']]

        tabla.columns = ['symbol_opc', 'strike', 'TTM', 'type', 'bid', 'ask', 'last', 'IV', 'openInt', 'theor',
                         'delta', 'gamma', 'theta', 'vega', 'rho', 'ITM']

        tabla['symbol'] = chain['symbol']
        tabla = tabla.set_index('symbol')

    except:
        tabla = pd.DataFrame()

    return tabla

def worker(tickers,nombre):

    tickers = tickers.tolist()

    for ticker in tickers:
        print(ticker, end=' ')
        cadena = options(ticker) #pido la cadena completa del ticker
        df = optionsDF(cadena) #paso la cadena a df
        if len(df) == 0:
            continue
        else:
            sqlDB.backup(df=df, nombre=nombre, if_exists="append")
    return df



def tablaCadenas(tickers, nombre="opciones_",key=keys.TOKEN_TD):
    import threading

    start_time = time.time()

    # creo la tabla por primera vez
    import datetime as dt
    hoy = str(dt.date.today())
    nombre = nombre+hoy
    sqlDB.tabla_cadenaOpciones(nombre=nombre)


    # descargo la info y la voy subiendo a la db

    noEncontrados = []
    n_threads = 2
    subs = np.array_split(tickers, n_threads) # separo en cantidad de listas segun cantidad de threads

    # hilos
    t0 = time.time()
    threads = []
    for i in range(n_threads):
        t = threading.Thread(target=worker, args=((subs[i]),nombre))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()


    print("--- %s seconds ---" % (time.time() - start_time))

    return noEncontrados