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
    ar = arg.replace(" ", '').replace("-", '').replace("(", '').replace(")", '')
    url = "http://num.voxlink.ru/get/"

    querystring = {"num": f"{ar}"}

    payload = ""
    response = requests.request("GET", url, data=payload, params=querystring)

    return response.json()


def get_page_data(html):
    global mail_2, number, city, operator
    hrefs_data = []
    soup = BeautifulSoup(html, 'lxml')
    ads = soup.find('grouping').find_all('group')
    for ad in ads:
        name = ad.find('title').text.replace('...', '')
        description = ad.find('passage').text.replace('...', '')
        hrefs = ad.find('url').text
        if re.search(r'\bРемонт офисов\b', name) or re.search(r'\bремонт офисов\b', name):
            hrefs_data.append(hrefs)
            for hre in hrefs_data:
                try:
                    r = requests.get(hre, headers=headers)
                    soups = BeautifulSoup(r.text, 'lxml')
                    try:
                        mail = soups.find(string=re.compile('\w+\@\w+.\w+')).text.strip()
                        if mail:
                            if len(mail) < 25:
                                mail_2 = mail.strip()
                            else:
                                mail_2 = soups.find('a', string=re.compile('\w+\@\w+.\w+')).text
                        else:
                            mail_2 = soups.find('a', string=re.compile('\w+\@\w+.\w+')).text
                    except:
                        mail_2 = ''
                    try:
                        try:
                            number = soups.find("a", string=re.compile('(\+7|8).\D*\d{3}\D*\d{3}\D*\d{2}\D*\d{2}')).text.strip()
                            jso = scan_tel(number)
                            city = jso['region']
                            operator = jso['operator']
                        except:
                            number = soups.find(string=re.compile('(\+7|8).\D*\d{3}\D*\d{3}\D*\d{2}\D*\d{2}')).text.strip()
                            jso = scan_tel(number)
                            city = jso['region']
                            operator = jso['operator']
                    except:
                        number = ''
                        jso = ''
                        city = ''
                        operator = ''
                except Exception as ex:
                    continue
            data = [(name, mail_2, city, number, operator, hrefs, description)]
            print(f"{GREEN}[+] Title:{RESET} {name}\n"
                  f"{GREEN}[+] Mail:{RESET} {mail_2}\n"
                  f"{GREEN}[+] City:{RESET} {city}\n"
                  f"{GREEN}[+] Number:{RESET} {number}\n"
                  f"{GREEN}[+] Operator:{RESET} {operator}\n"
                  f"{GREEN}[+] Url:{RESET} {hrefs}\n"
                  f"{GREEN}[+] Description:{RESET} {description}")
            print(f"{YELLOW}#" * 70)
            insert_db(data)
        else:
            pass


def insert_db(data):
    # p = os.path.abspath('mydatabase_Stoit_Kompani.db')
    connection = sqlite3.connect("mydatabase_Stoit_Kompani_test.db")
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
    get_page_data(html)


def main():
    global url
    numb = [1, 10645, 10650, 10658, 10672, 10687, 10693, 10699, 10705, 10712, 10772, 10776, 10795, 10802, 10819, 10832,
            10841, 10842, 10176, 10853, 10857, 10933, 10939, 10939, 10174, 10897, 10904, 10926, 11004, 10946, 10950, 11015,
            10995, 977, 11029, 11111, 11070, 11077, 11117, 11079, 11084, 11095, 11108, 11131, 11146, 11119, 11148,
            11153, 11156, 11235, 10231, 11330, 21949, 11266, 11282, 11309, 11316, 11318, 11353, 10233, 11340, 11158, 11162, 11176, 11232, 11193, 11225, 11375, 10243, 11398,
            11403, 11409, 11443, 11450, 11457, 10251, 11010, 11012, 11013, 11020, 11021, 11069, 11024
            ]

    for n in numb:
        url = f'http://xmlriver.com/search_yandex/xml?user=6019&key=10e92a3587615ff8137a89723928af6c1c4ef396&query=ремонт%20офисов&page=0&lr={n}'
        text = re.sub(r'page=\d+', 'page={}', url)
        # for i in range(31):
        #     text = re.sub(r'page=\d+', f'page={i}', url)
        #     get_page_data(get_html(text))

        urls = [text.format(str(i)) for i in range(31)]
        # Подключаем мультипроцессинг
        with Pool(6) as p:
            p.map_async(make_all, urls, callback=end_func)
            p.close()
            p.join()


if __name__ == '__main__':
    main()
