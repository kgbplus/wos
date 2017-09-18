from bs4 import BeautifulSoup
import requests
import re

req = requests.get("http://www.chessgames.com/perl/chesscollection?cid=1014492")
soup = BeautifulSoup(req.text, "lxml")

pages = soup.findAll('a', href=re.compile('.*chessgame\?.*'))

def download_file(url):
    path = url.split('/')[-1].split('?')[0]
    r = requests.get(url, stream=True)
    if r.status_code == 200:
        with open(path, 'wb') as f:
            for chunk in r:
                f.write(chunk)

host = 'http://www.chessgames.com'
for page in pages:
    url = host + page.get('href')
    req = requests.get(url)
    soup = BeautifulSoup(req.text, "lxml")
    file_link = soup.find('a',text=re.compile('.*download.*'))
    file_url = host + file_link.get('href')
    download_file(file_url)
