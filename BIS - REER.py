import os,pandas as pd
import matplotlib.pyplot as plt

os.getcwd()

df = pd.read_csv("C:/Users/ahlan/Downloads/bis_reer.csv",header='infer')
df['Time Period'] = pd.to_datetime(df['Time Period'],format='%d/%m/%Y')
df = df.set_index(df[df.columns[0]]).drop(df.columns[0],axis=1).fillna(method='ffill')

for k in df.columns:
    print(k[:2])
#"AU","CZ","DE","MX","PL","NO","SI","TH","TW","US","XM"
countries = ["SE","ZA","SG","TH","TW","US","XM","TR"]#"AU","CZ","DK","MX","PL","NO","HU","US"]#"SE","ZA","SG","TH","TW","US","XM","TR"]
l = []
for k in countries:
    d = df[df.columns[df.columns.str.contains(k) == True]]
    if len(d)>1:
        d = d[d.columns[0]]
        l.append(d)
    else:l.append(d)
d = pd.concat(l,axis=1,join='inner')
dd  = d[-500:]
dd2 =dd.drop('US:United States',axis=1)
dd_us = dd['US:United States']
ddd = dd2.apply(lambda x:x.pct_change()-dd_us.pct_change())
ddd.rolling(55,10).std().diff().cumsum().plot(subplots=False,legend=True)
plt.axhline(0,color='black')
plt.legend(loc='upper left')