import time
import requests
from bs4 import BeautifulSoup
import sqlite3
import re
from multiprocessing import Pool
import random
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


def retrys(func, retries=5):
    def retry_wrapper(*args, **kwargs):
        attempts = 0
        while attempts < retries:
            try:
                return func(*args, **kwargs)
            except requests.exceptions.RequestException as e:
                print("Пытаюсь восстановить подключение!")
                time.sleep(10)
                attempts += 1

    return retry_wrapper


@retrys
def get_html(url, retry=5):  # Делаем запрос к странице
    try:
        r = requests.get(url, headers=headers)
        print(f'Выполняется парсинг данной страницы {url}')
    except Exception as ex:
        time.sleep(5)
        if retry:
            print(f"INFO retry={retry} => {url}")
            return get_html(url, retry=(retry - 1))
        else:
            raise
    else:
        return r.text


def scan_tel(arg):
    data_num = {
        "number": f"{arg}"
    }
    url = 'https://www.kody.su/check-tel'
    r = requests.post(url, data=data_num, headers=headers)
    soup = BeautifulSoup(r.text, 'lxml')
    ads = soup.find('div', class_='content__in').find_all('strong')
    return ads[1].text.replace(',', '')


def get_page_data(html):
    global mail_2, number, city
    hrefs_data = []
    soup = BeautifulSoup(html, 'lxml')
    ads = soup.find('grouping').find_all('group')
    for ad in ads:
        name = ad.find('title').text.replace('...', '')
        description = ad.find('passage').text.replace('...', '')
        hrefs = ad.find('url').text
        if re.search(r'\bСтроительная компания\b', name):
            hrefs_data.append(hrefs)
            for hre in hrefs_data:
                try:
                    r = requests.get(hre, headers=headers)
                    soups = BeautifulSoup(r.text, 'lxml')
                    try:
                        mail = soups.find(string=re.compile('\w+\@\w+.\w+')).text.strip()
                        if len(mail) < 25:
                            mail_2 = mail.strip()
                    except:
                        mail_2 = ''
                    try:
                        number = soups.find("a", string=re.compile('(\+7|8).\D*\d{3}\D*\d{3}\D*\d{2}\D*\d{2}')).text.strip()
                        city = scan_tel(number)
                    except:
                        number = ''
                        city = ''
                except Exception as ex:
                    continue
            data = [(name, mail_2, city, number, hrefs, description)]
            print(f"{GREEN}[+] Title:{RESET} {name}\n"
                  f"{GREEN}[+] Mail:{RESET} {mail_2}\n"
                  f"{GREEN}[+] City:{RESET} {city}\n"
                  f"{GREEN}[+] Number:{RESET} {number}\n"
                  f"{GREEN}[+] Url:{RESET} {hrefs}\n"
                  f"{GREEN}[+] Description:{RESET} {description}")
            print("#"*70)
            insert_db(data)
        else:
            pass


def insert_db(data):
    connection = sqlite3.connect('mydatabase_Stoit_Kompani.db')
    cursor = connection.cursor()
    urls = [x[4] for x in data]
    for s in urls:
        pass
    cursor.execute(f"SELECT hrefs FROM pars WHERE hrefs = '{s}'")
    if cursor.fetchone() is None:
        cursor.executemany("INSERT INTO pars VALUES (?,?,?,?,?,?)", data)
        connection.commit()
        print("Запись Добавлена ")
    else:
        print("Уже есть")


def end_func(response):
    print("Задание завершено")


def make_all(url):
    html = get_html(url)
    get_page_data(html)


def main():
    global url
    numb = [3, 225, 17, 26, 40, 59, 52, 73, 102444, 10712, 10772, 10776, 10795, 10802, 10819, 10832, 10841]
    for n in numb:
        url = f'http://xmlriver.com/search_yandex/xml?user=6019&key=10e92a3587615ff8137a89723928af6c1c4ef396&query=строительная%20компания&page=1&lr={n}'
        #rand = random.choice(numb)
        #url = f'http://xmlriver.com/search_yandex/xml?user=6019&key=10e92a3587615ff8137a89723928af6c1c4ef396&query=строительная%20компания&page=1&lr={rand}'
        text = re.sub(r'page=\d+', 'page={}', url)
        urls = [text.format(str(i)) for i in range(1, 31)]
        print(urls)

        # Подключаем мультипроцессинг
        with Pool(2) as p:
            p.map_async(make_all, urls, callback=end_func)
            p.close()
            p.join()


if __name__ == '__main__':
    main()
