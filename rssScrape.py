######################## RSS FEED PARSING ################
import feedparser
from bs4 import BeautifulSoup
from datetime import datetime,timezone
from time import mktime
def utc_to_local(utc_dt):
    return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=None)
urlFeeds = ["http://feeds.reuters.com/reuters/businessNews",
            "http://feeds.reuters.com/reuters/companyNews",
            "http://feeds.reuters.com/news/wealth",
            "http://feeds.reuters.com/Reuters/PoliticsNews",
            "http://feeds.reuters.com/reuters/technologyNews",
            "http://feeds.reuters.com/reuters/topNews",
            "http://feeds.reuters.com/Reuters/worldNews",
            "https://www.channelnewsasia.com/rssfeeds/8395744",
            "https://www.channelnewsasia.com/rssfeeds/8395986",
            "https://www.channelnewsasia.com/rssfeeds/8395954",
            "https://www.channelnewsasia.com/rssfeeds/8395884",
            "http://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
            "http://rss.nytimes.com/services/xml/rss/nyt/SmallBusiness.xml",
            "http://rss.nytimes.com/services/xml/rss/nyt/Economy.xml",
            "https://rss.nytimes.com/services/xml/rss/nyt/Dealbook.xml",
            "http://rss.nytimes.com/services/xml/rss/nyt/MediaandAdvertising.xml",
            "http://rss.nytimes.com/services/xml/rss/nyt/YourMoney.xml",
            "http://rss.nytimes.com/services/xml/rss/nyt/Technology.xml",
            "http://rss.nytimes.com/services/xml/rss/nyt/PersonalTech.xml",
            "http://rss.nytimes.com/services/xml/rss/nyt/Upshot.xml",
            "http://rss.nytimes.com/services/xml/rss/nyt/Politics.xml",
            "http://rss.nytimes.com/services/xml/rss/nyt/AsiaPacific.xml",
            "http://rss.nytimes.com/services/xml/rss/nyt/Europe.xml",
            "http://rss.nytimes.com/services/xml/rss/nyt/MiddleEast.xml",
            "http://rss.nytimes.com/services/xml/rss/nyt/Americas.xml",
            "https://www.cnbc.com/id/38818154/device/rss/rss.html",
            "https://www.cnbc.com/id/20398120/device/rss/rss.html",
            "https://www.cnbc.com/id/15839069/device/rss/rss.html",
            "https://www.cnbc.com/id/10000113/device/rss/rss.html",
            "https://www.cnbc.com/id/10000664/device/rss/rss.html",
            "https://www.cnbc.com/id/20910258/device/rss/rss.html",
            "https://www.cnbc.com/id/100370673/device/rss/rss.html",
            "https://www.cnbc.com/id/15839135/device/rss/rss.html",
            "https://www.cnbc.com/id/10001147/device/rss/rss.html",
            "https://www.ft.com/global-economy?format=rss",
            "https://www.ft.com/world?format=rss",
            "https://www.ft.com/reports/hedge-fund-strategies?format=rss",
            "https://www.ft.com/markets?format=rss",
            "https://www.ft.com/technology?format=rss",
            "https://www.ft.com/?format=rss",
            "https://feeds.a.dj.com/rss/RSSWorldNews.xml",
            "https://feeds.a.dj.com/rss/RSSOpinion.xml",
            "https://feeds.a.dj.com/rss/RSSWSJD.xml",
            "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
            ]
#import feedparser
#newsfeed = "https://www.upstreamonline.com/rss2/"
# rss = feedparser.parse(newsfeed)
# for k in rss.entries:
#    print(k.title,"-",k.summary)
feeds = []
for j in urlFeeds:
    rss = feedparser.parse(str(j))
    for k in range(len(rss['entries'])):
        try:
            date = datetime.fromtimestamp(mktime(rss['entries'][k]['published_parsed']))
            date = utc_to_local(date).strftime('%Y-%m-%d %H:%M:%S %z')
            title = rss['entries'][k]['title'].rstrip()
            text = BeautifulSoup(rss['entries'][k].summary,'html.parser').getText().rstrip()
            link = rss['entries'][k]['link']
            header_category =rss.feed.title.rstrip()
            try:
                g = []
                for i in range(len(rss['entries'][k].tags)):
                    tags = rss['entries'][k].tags[i].term.strip()
                    g.append(tags)
                    g = ", ".join(g)
            except:pass
            l = {"title":title,"text":text,"date_published":date,"source":link,"header_category":header_category,"tags":g}
            feeds.append(l)
        except:continue
import pandas as pd
import string

df = pd.DataFrame(feeds).drop_duplicates(subset='title')
df = df[~df.source.str.contains("video")].astype('str')

def pushToSql(df):
    import pyodbc,sqlalchemy
    cxn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};'
                         'SERVER=LAPTOP-MGHPU5NH\SQLEXPRESS;'
                         'DATABASE=rssNews;Trusted_Connection=yes')
    engine = sqlalchemy.create_engine("mssql+pyodbc://LAPTOP-MGHPU5NH\SQLEXPRESS/rssNews?driver=SQL server")
    df.to_sql(name='rssNews',con=engine,if_exists='append')
    cxn.close()
today = df[df.date_published.str.contains(datetime.now().strftime("%Y-%m-%d"))]
#missed_dates = ["2019-09-15"]
missed_dates = ["2019-10-17","2019-10-18","2019-10-19","2019-10-20","2019-10-21","2019-10-22"]
for kkj in range(len(missed_dates)):
    yesterday = df[df.date_published.str.contains(missed_dates[kkj])]
    pushToSql(yesterday)
#pushToSql(today)
###update only the latest news i.e. Today's News
# df.to_csv("scraped.csv",index=False,header=True,encoding='utf-8-sig')
# today.to_csv("today_scraped.csv",index=False,header=True,encoding='utf-8-sig')
# l = []
# for k in ['ft.com','wsj.com']:
#     l.append(df[df['source'].str.contains(k)])
# l=pd.concat(l,sort=True)
# ll=l[~l['date_published'].str.contains(datetime.now().strftime("%Y-%m-%d"))]

