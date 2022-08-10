import time
import requests
from bs4 import BeautifulSoup
import sqlite3
import re
from multiprocessing import Pool
import colorama


# запускаем модуль colorama
colorama.init()

GREEN = colorama.Fore.GREEN
GRAY = colorama.Fore.LIGHTBLACK_EX
RESET = colorama.Fore.RESET
YELLOW = colorama.Fore.YELLOW

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
}



def get_html(url):  # Делаем запрос к странице
    try:
        r = requests.get(url, headers=headers)
        if r.ok:
            #print(f'{r.status_code} url: {url} ')
            return url
    except Exception as e:
        pass


item = 0
def get_page_data(html):
    global item
    if html != None:
        with open('../Search/200_ok.txt', 'a')as f:
            f.write(f"{html.strip()}\n")
            item += 1
            print(f"{item} {html}")



def end_func(response):
    print("Задание завершено")


def make_all(url):
    html = get_html(url)
    get_page_data(html)


def main():
    url_data = []
    with open("../Search/ru_domains_base.txt", "r") as f:
        for line in f:
            url = 'https://' + line.lower().strip()
            url_data.append(url)

        # Подключаем мультипроцессинг
        with Pool(20) as p:
            p.map_async(make_all, url_data, callback=end_func)
            p.close()
            p.join()




if __name__ == '__main__':
    main()
