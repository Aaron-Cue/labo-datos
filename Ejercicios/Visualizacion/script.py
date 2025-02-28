
import pandas as pd
import matplotlib.pyplot as plt

biodiesel_data = pd.read_csv('./precioBiodiesel.csv')

fig, ax = plt.subplots()

ax.plot(biodiesel_data[::-1][::4]['Periodo'], biodiesel_data[::-1][::4]['Precio'], data = biodiesel_data, marker ='o')

ax.set_xlabel("Anios")
ax.set_ylabel("Precios")
ax.set_title("Precios biodiesel")

plt.xticks(rotation=45) # rotar etiquetas

#%%

data_tel = pd.read_csv('./telefonosInteligentes.csv')
fig, ax = plt.subplots()

data_tel.plot(x='RangoEtario', kind='bar', label=['TelefonoInteligente', 'TelefonoNoInteligente', 'SinTelefono'], ax=ax)
ax.set_ylim(0, data_tel.iloc[:, 1:].max().max() * 1.5)


#%%