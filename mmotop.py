from bs4 import BeautifulSoup
import requests

session = requests.Session()
headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
           'Accept-Encoding': 'gzip, deflate',
           'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
           'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:49.0) Gecko/20100101 Firefox/49.0'}

req = session.get("https://mmotop.ru/users/sign_in", headers=headers)
soup = BeautifulSoup(req.text, "lxml")

token = soup.find('input', {'name': 'authenticity_token'}).get('value')

params = {'utf8': '&#x2713;',
          'authenticity_token': token,
          'user[remember_me]': 'true',
          'user[email]': 'm@mail.ru',
          'user[password]': '---',
          'sign_in': 'Войти'}

req = session.post("https://mmotop.ru/users/sign_in", params=params, headers=headers)
