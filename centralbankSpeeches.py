import requests,os,re
import feedparser
# from string import punctuation
from datetime import datetime,timezone

def makeFolder(filedate): ###works

    try:
        os.mkdir(flib+filedate)##put date here 20190630
    except FileExistsError:
        pass
def utc_to_local(utc_dt):
    return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=None)

fp = "C:/Users/ahlan/Desktop/publications/centralbankSpeeches/"
flib = "C:/Users/ahlan/Desktop/publications/centralbankSpeeches/pdf/"
pgList = list(range(1,3))
l = []
for page in pgList:
    # print(page)
    url = "https://www.bis.org/doclist/cbspeeches.rss?page="+str(page)+"&paging_length=25"
    p = feedparser.parse(url)
    for k in range(len(p.entries)):
        try:
            # fn = "_".join(p.entries[k].cb_simpletitle.translate(str.maketrans('','',punctuation)).split(' ')[:6])
            fn = p.entries[k].author.replace(' ','_')
            file_url = str(p.entries[k].cb_link)
            date_published = datetime.strptime(re.search(r".*T",p.entries[k].cb_occurrencedate).group(0)[:-1],"%Y-%m-%d")
            folder_date = datetime.strftime(date_published, "%Y%m%d")
            date_published = utc_to_local(date_published).strftime('%Y-%m-%d %H:%M:%S %z')
            title = p.entries[k].cb_simpletitle
            d = {"author":fn, "title":title,"BISsource":file_url,"date_published":date_published,"folderLoc":str(folder_date+"_"+fn)}

            if not os.path.exists(str(flib+folder_date+'/'+fn+".pdf")):
                l.append(d)
                makeFolder(folder_date)
                r = requests.get(file_url, stream=True)
                with open(str(flib+folder_date+'/'+fn+".pdf"), "wb") as pdf:
                    for chunk in r.iter_content(chunk_size=2048):

                        # writing one chunk at a time to pdf file
                        if chunk:
                            pdf.write(chunk)
        except:
            print("Check speech by {} on this {} at this {}".format(p.entries[k].author,p.entries[k].updated,p.entries[k].link))
requests.session().close()

import pandas as pd
def pushToSql(df):
    import pyodbc,sqlalchemy
    cxn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};'
                         'SERVER=LAPTOP-MGHPU5NH\SQLEXPRESS;'
                         'DATABASE=rssNews;Trusted_Connection=yes')
    engine = sqlalchemy.create_engine("mssql+pyodbc://LAPTOP-MGHPU5NH\SQLEXPRESS/rssNews?driver=SQL server")
    df.to_sql(name='centralBankSpeeches',con=engine,if_exists='append')
    cxn.close()

try:
    df = pd.DataFrame.from_dict(l)
    pushToSql(df)
except:
    print("Nothing to update pushed to sql server")

# import PyPDF2
# fn = "David_Ramsden_Resilience__three_lessons.pdf"
# file = fp+fn
# #open allows you to read the file
# pdfFileObj = open(file,'rb')
# #The pdfReader variable is a readable object that will be parsed
# pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
# num_pages = pdfReader.numPages
# set(pdfReader.getPage(1).extractText().replace('\n','').translate(str.maketrans('','',punctuation)).split(' '))
# def lastUpdate(scriptStart = True):
#     from datetime import datetime,timedelta
#     if scriptStart == True:
#         f1 = open(fp+'lastUpdated.txt','w+')
#         f1.write(datetime.strftime(datetime.now(),format="%d-%m-%Y"))
#         f1.close()
#     else: f1 = open(fp+'lastUpdated.txt','r+')
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