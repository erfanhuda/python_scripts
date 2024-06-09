import urllib.request
from html.parser import HTMLParser
from bs4 import BeautifulSoup

fp = urllib.request.urlopen("https://docs.google.com/spreadsheets/d/1FGJbcKXUGWXEVyfPNcOKxzpgy3Du1vOI/edit")
fp_str = fp.read()
fp_str = fp_str.decode("utf-8")

fp.close()


class Parser(HTMLParser):
    def handle_starttag(self, tag, attrs):
        print("Encountered a start tag:", tag)
        print("Encountered a start tag:", attrs)

    def handle_endtag(self, tag):
        print("Encountered an end tag :", tag)

    def handle_data(self, data):
        with open("file.txt", "w") as f:
            for item in data:
                f.write(data)

        print("Encountered some data  :", data)


if __name__ == '__main__':
    # parser = Parser()
    # parser.handle_starttag("table", [("class","waffle")])
    # parser.handle_endtag("table")
    # parser.feed(fp_str)

    soup = BeautifulSoup(fp_str, 'html.parser')
    table = soup.table.findAll('tr')
    # table = soup.table
    # print(dir(soup.table))
    new_td = []
    last_td = []
    for item in table:
        new_td.append(item.findAll('td'))

    # for i, item in enumerate(new_td):
    #     last_td.append(item[:1].findAll('td'))

    # for i, item in enumerate(last_td):
    #     if i % 2 == 0:
    #         # item[:7].findAll('td')
    #         print(item[:7].findAll('td'))
    
    print(new_td[4][0].find('td'))
    
    # with open("test.html", "w+") as f:
    #     f.write(fp_str)
    # print(text)
    # with open("file.txt", "w") as f:
    #     f.write(str(parser.feed(fp_str)))