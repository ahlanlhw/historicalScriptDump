import os,PyPDF2

fp = "C:/Users/ahlan/Desktop/publications/centralbankSpeeches/pdf/"
folder_list = [k[0] for k in os.walk(fp)]
cp = folder_list[-2]
os.chdir(cp)

with open(os.listdir()[-1],'rb') as f:
    pdf = PyPDF2.PdfFileReader(f)
    pdf.strict = False
    for k in range(pdf.getNumPages()):
        p = pdf.getPage(k)
        print(p.extractText().encode('utf-8'))