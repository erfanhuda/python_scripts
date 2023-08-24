import requests, zipfile, io
from bs4 import BeautifulSoup
import tkinter as tk
from tkinter import ttk
import warnings

warnings.filterwarnings("ignore")

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.7600.43 Safari/537.36"}

def buku3_parser(month, year):
    try:
        URL = f"https://ojk.go.id/id/kanal/perbankan/data-dan-statistik/statistik-perbankan-indonesia/Documents/Pages/Statistik-Perbankan-Indonesia---{month}-{year}/STATISTIK%20PERBANKAN%20INDONESIA%20-%20{month}%20{year}.zip"
        test_conn = requests.get(URL, stream=True)

        z = zipfile.ZipFile(io.BytesIO(test_conn.content))
        z.extractall("./result")

    except requests.ConnectionError:
        print("Connection error occurred. Ensure you are connected to internet.")
    
    except zipfile.BadZipFile:
        print("Error occurred. Whether the month not already exists or the URL may broken.")

def chart_buku3_parser(month, year):
    try:
        URL = f"https://ojk.go.id/id/kanal/perbankan/data-dan-statistik/statistik-perbankan-indonesia/Pages/Statistik-Perbankan-Indonesia---{month}-{year}.aspx"
        test_conn = requests.get(URL, headers=HEADERS)
        engine = BeautifulSoup(test_conn.content, "html.parser")
        img = engine.find_all("div", class_ = "ms-rtestate-field")
        list_p = img[-1].find_all("p")
        list_img = [x.img for x in list_p[1:]]
        url_img = ['https://ojk.go.id{}'.format(x['src']) for x in list_img]


        for url in url_img:
            requests.get(url, headers=HEADERS)
            with open(url.title(), 'wb') as f:
                print(url_img)
    except requests.ConnectionError:
        print("Connection error occurred. Ensure you are connected to internet.")
    
def pefindo_ratings():
    try:
        URL = f"https://pefindo.com/pageman/page/all-rating.php?fullpage=1&id=[%222%22,%226%22,%228%22,%229%22]&id=%5B2%2C6%2C8%2C9%5D"
        soup = requests.get(URL, headers=HEADERS)
        engine = BeautifulSoup(soup.content, "html.parser")
        tables = engine.find("table", class_="finance-tb")
        thead = tables.thead.tr.find_all("th")
        
        tbody = tables.tbody.find_all("tr")
        company_names = [name.find_all("td") for name in tbody]
        items = [{thead[0].text: item[0].text, thead[1].text: item[1].text, thead[2].text: item[2].text} for item in company_names]

        return items
    
    except requests.ConnectionError:
        print("Connection error occurred. Ensure you are connected to internet.")

def enlist_all_banks():
    try:
        URL = f"https://pefindo.com/pageman/page/financial-institutions-ratings.php?id=2&comp=1&id=33&"
        request = requests.get(URL, headers=HEADERS)
        soup = BeautifulSoup(request.content, "html.parser")
        tables = soup.find("table", class_="finance-tb").find("tbody")
        results = [(item.a['href'].split("id=")[1], item.a.text, item.a['href']) for item in tables.find_all("tr", class_="odd")] + [(item.a['href'].split("id=")[1], item.a.text, item.a['href'])for item in tables.find_all("tr", class_="even")]
        
        return results

    except requests.ConnectionError:
        print("Connection error occurred. Ensure you are connected to internet.")

def enlist_all_corporates_industry():
    try:
        URL = f"https://pefindo.com/pageman/page/corporates-ratings.php?id=1"
        request = requests.get(URL, headers=HEADERS)
        soup = BeautifulSoup(request.content, "html.parser")
        tables = soup.find("table", class_="finance-tb").find("tbody")
        results = [(item.a['href'].split("&id=")[1].split("&")[0], item.a.text, item.a['href']) for item in tables.find_all("tr", class_="odd")] + [(item.a['href'].split("&id=")[1].split("&")[0], item.a.text, item.a['href'])for item in tables.find_all("tr", class_="even")]

        return results

    except requests.ConnectionError:
        print("Connection error occurred. Ensure you are connected to internet.")

def current_rate_bi():
    try:
        URL = "https://www.bi.go.id/id/statistik/informasi-kurs/transaksi-bi/Default.aspx"
        request = requests.get(URL,headers=HEADERS, verify=False, stream=True, allow_redirects=True, timeout=5)
        soup = BeautifulSoup(request.content, "html.parser")
        # date = soup.find("div", class_="row").find("div", class_="form-group").find("div", class_="search-box-wrapper")
        dates = soup.find_all("span")
        data = soup.find("tbody").find_all("tr")

        for item in dates:
            print(item.text)
        

    except requests.ConnectionError:
        print("Connection error occurred. Ensure you are connected to internet.")

def current_rate_pajak():
    try:
        URL = f"https://pefindo.com/pageman/page/corporates-ratings.php?id=1"
        request = requests.get(URL, verify=False)
        soup = BeautifulSoup(request.content, "html.parser")
        date = soup.find("div", class_="mr-4")
        
        print(date.span.text)

    except requests.ConnectionError:
        print("Connection error occurred. Ensure you are connected to internet.")



class App(tk.Tk):
    def __init__(self, *args, **kwargs):
        super().__init__()
        self.title("Window Application")
        self.geometry("1200x600")


if __name__ == "__main__":
    # app = App()
    # app.mainloop()
    current_rate_bi()