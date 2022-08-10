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


def retrys(func, retries=5):
    def retry_wrapper(*args, **kwargs):
        attempts = 0
        while attempts < retries:
            try:
                return func(*args, **kwargs)
            except requests.exceptions.RequestException as e:
                print("Не удалось восстановить подключение!")
                time.sleep(60)
                attempts += 1

    return retry_wrapper


@retrys
def get_html(url, retry=5):  # Делаем запрос к странице
    try:
        session = requests.session()
        r = session.get(url, headers=headers)
        #print(f'Выполняется парсинг данной страницы {url}')
    except Exception as ex:
        time.sleep(60)
        if retry:
            print(f"INFO retry={retry} => {url}")
            return get_html(url, retry=(retry - 1))
        else:
            raise
    else:
        return r.text


def scan_tel(arg):
    ar = arg.replace(" ", '').replace("-", '').replace("(", '').replace(")", '')
    url = "http://num.voxlink.ru/get/"
    querystring = {"num": f"{ar}"}
    payload = ""
    response = requests.request("GET", url, data=payload, params=querystring)
    return response.json()


def get_page_data(html, url):
    global mail_2, number, city, operator, number_one, descr
    soup = BeautifulSoup(html, 'lxml')
    name = soup.find('title').text
    descriptions = soup.find('head').find_all(attrs={"name": "description"})
    for descript in descriptions:
        descr = descript.get('content')
    if re.search(r'\bСтроительная компания\b', name) or re.search(r'\bстроительная компания\b', name):
            try:
                mail = soup.find(string=re.compile('\w+\@\w+.\w+')).text.strip()
                if mail:
                    if len(mail) < 25:
                        mail_2 = mail.strip()
                    else:
                        mail_2 = soup.find('a', string=re.compile('\w+\@\w+.\w+')).text
                else:
                    mail_2 = soup.find('a', string=re.compile('\w+\@\w+.\w+')).text
            except:
                mail_2 = ''
            try:
                try:
                    number = soup.find("a",
                                        string=re.compile('(\+7|8).\D*\d{3}\D*\d{3}\D*\d{2}\D*\d{2}')).text.strip()
                    jso = scan_tel(number)
                    city = jso['region']
                    operator = jso['operator']
                except:
                    number = soup.find(string=re.compile('(\+7|8).\D*\d{3}\D*\d{3}\D*\d{2}\D*\d{2}')).text.strip()
                    jso = scan_tel(number)
                    city = jso['region']
                    operator = jso['operator']
            except:
                number = ''
                jso = ''
                city = ''
                operator = ''
            data = [(name, mail_2, city, number, operator, html, descr)]
            print(f"{GREEN}[+] Title:{RESET} {name}\n"
                  f"{GREEN}[+] Mail:{RESET} {mail_2}\n"
                  f"{GREEN}[+] City:{RESET} {city}\n"
                  f"{GREEN}[+] Number:{RESET} {number}\n"
                  f"{GREEN}[+] Operator:{RESET} {operator}\n"
                  f"{GREEN}[+] Url:{RESET} {url} \n"
                  f"{GREEN}[+] Description:{RESET} {descr}")
            print(f"{YELLOW}#" * 70)
            #insert_db(data)
    else:
        pass






def insert_db(data):
    # p = os.path.abspath('mydatabase_Stoit_Kompani.db')
    connection = sqlite3.connect("mydatabase_Stoit_Kompani.db")
    cursor = connection.cursor()
    urls = [x[5] for x in data]
    for s in urls:
        pass
    cursor.execute(f"SELECT hrefs FROM pars WHERE hrefs = '{s}'")
    if cursor.fetchone() is None:
        cursor.executemany("INSERT INTO pars VALUES (?,?,?,?,?,?,?)", data)
        connection.commit()
        print(f"{RESET}Запись Добавлена ")
    else:
        print(f"{RESET}Уже есть")


def end_func(response):
    print("Задание завершено")


def make_all(url):
    html = get_html(url)
    get_page_data(html, url)


def main():
    # url = 'https://kvsspb.ru/'
    # get_page_data(get_html(url))
    url_data = []
    with open("200_ok.txt", "r") as f:
        for line in f:
            url = line.replace('\n', '')
            url_data.append(url)

        # Подключаем мультипроцессинг
        with Pool(40) as p:
            p.map_async(make_all, url_data, callback=end_func)
            p.close()
            p.join()


if __name__ == '__main__':
    main()
