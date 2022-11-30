import requests,os
import feedparser,re,random,time
from datetime import datetime,timezone
from time import mktime
def utc_to_local(utc_dt):
    return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=None)
os.getcwd()
# fp = "C:/Users/ahlan/Desktop/publications/centralbankSpeeches/"
# url = 'https://trends.google.com/trends/explore?geo='+ctry+'&q='+qry ###' ' has to be represented by '%20' "lee%20kuan%20yew"
ctryCode = ['AU','CA','US','SG','NZ','UK']
l = []
for ctry in ctryCode:
    # print(ctry)
    url = "https://trends.google.com/trends/trendingsearches/daily/rss?geo="+ctry
    p = feedparser.parse(url)
    for k in range(len(p.entries)):
        time.sleep(random.uniform(0,2.5))
        try:
            title = p.entries[k].title
            raw_snippet = re.compile(r'<[^>]+>').sub('',p.entries[k].ht_news_item_snippet)
            trfc = int(p.entries[k].ht_approx_traffic.strip('+').replace(',',''))
            date_published = datetime.fromtimestamp(mktime(p.entries[k].published_parsed))
            date_published = utc_to_local(date_published).strftime('%Y-%m-%d %H:%M:%S %z')
            source = p.entries[k].ht_news_item_source
            link = p.entries[k].link
            country = ctry
            l.append({"title":title,"raw_text":raw_snippet,"traffic":trfc,"date_published":date_published,"source":source,"link":link,"country":ctry})
        except:
            print("Entry {0} in {1} not appending".format(k,ctry))
            continue
import pandas as pd
df = pd.DataFrame.from_dict(l)

# def lastUpdate(scriptStart = True):
#     from datetime import datetime,timedelta
#     if scriptStart == True:
#         f1 = open(fp+'googleTrendsUpdate.txt','w+')
#         f1.write(datetime.strftime(datetime.now(),format="%d-%m-%Y"))
#         f1.close()
#     else: f1 = open(fp+'googleTrendsUpdate.txt','r+')
#         lastUpdated = f1.readlines()[0]
#         lastUpdated = datetime.strptime(lastUpdated,"%d-%m-%Y")
#         delta = datetime.now() - lastUpdated
#         datelist=[]
#         for k in range(1,delta.days+1):
#             datelist.append(lastUpdated + timedelta(days=k))
#         f1.close()
#         return datelist
#
# lastUpdate(scriptStart=False)
def pushToSql(df):
    import pyodbc,sqlalchemy
    cxn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};'
                         'SERVER=LAPTOP-MGHPU5NH\SQLEXPRESS;'
                         'DATABASE=rssNews;Trusted_Connection=yes')
    engine = sqlalchemy.create_engine("mssql+pyodbc://LAPTOP-MGHPU5NH\SQLEXPRESS/rssNews?driver=SQL server")
    df.to_sql(name='googleTrends',con=engine,if_exists='append')
    cxn.close()
#missed_dates = ["2019-09-15"]
missed_dates = ["2019-10-17","2019-10-18","2019-10-19","2019-10-20","2019-10-21","2019-10-22"]
#today = df[df.date_published.str.contains(datetime.now().strftime("%Y-%m-%d"))]
for kkj in range(len(missed_dates)):
    yesterday = df[df.date_published.str.contains(missed_dates[kkj])]
    pushToSql(yesterday)
#pushToSql(today)