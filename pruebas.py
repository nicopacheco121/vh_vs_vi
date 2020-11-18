import pandas as pd

"""
data = pd.read_excel("AAPL.xlsx")
data = data.sort_values("timestamp",ascending=True)
data.drop(["open", "high", "low", "close", "volume"],axis=1, inplace=True)
data["change"] = data.adjusted_close.pct_change()
data["vh75"] = (data.change.rolling(75).std()) * (252**0.5)
data = data.dropna()

print(data)

# CODIGO PARA HACER LA TABLA DE ESTADISTICA
tickers = ["AAPL"]
lista = []
for ticker in tickers:
    dic = {}
    dic["ticker"] = ticker
    dic["vh"] = data.vh75.mean()
    dic["kurtosis"] = data.vh75.kurtosis()
    dic["skew"] = data.vh75.skew()
    dic["0.1"] = data.vh75.quantile(q = 0.1)
    dic["0.2"] = data.vh75.quantile(q = 0.2)
    dic["0.3"] = data.vh75.quantile(q = 0.3)
    dic["0.4"] = data.vh75.quantile(q = 0.4)
    dic["0.5"] = data.vh75.quantile(q = 0.5)
    dic["0.6"] = data.vh75.quantile(q = 0.6)
    dic["0.7"] = data.vh75.quantile(q = 0.7)
    dic["0.8"] = data.vh75.quantile(q = 0.8)
    dic["0.9"] = data.vh75.quantile(q = 0.9)

    lista.append(dic)

lista = pd.DataFrame(lista)
print(lista.round(4))

"""


