import multiprocessing
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
        session = requests.session()
        session.headers =headers
        r = session.get(url, headers=headers)
        if r.status_code == 200:
            print(f'Выполняется парсинг данной страницы {url}')
            return r.text
    except requests.exceptions.HTTPError as errh:
        print("Http Error:", errh)
    except requests.exceptions.ConnectionError as errc:
        pass
    except requests.exceptions.Timeout as errt:
        print("Timeout Error:", errt)
    except requests.exceptions.RequestException as err:
        print("OOps: Something Else", err)




def scan_tel(arg):
    ar = arg.replace(" ", '').replace("-", '').replace("(", '').replace(")", '')
    url = "http://num.voxlink.ru/get/"
    querystring = {"num": f"{ar}"}
    payload = ""
    response = requests.request("GET", url, data=payload, params=querystring)
    return response.json()


def get_page_data(html, url):
    global mail_2, number, city, operator, number_one, descr
    if html != None:
        soup = BeautifulSoup(html, 'lxml')
        try:
            name = soup.find('title').text
        except:
            name = ''
        try:
            descriptions = soup.find('head').find_all(attrs={"name": "description"})
        except:
            descriptions = ''
        for descript in descriptions:
            descr = descript.get('content')
        if re.search(r'\bСтроительная компания\b', name) or re.search(r'\bстроительная компания\b', name) or re.search(r'\bРемонт квартир\b', name)\
                or re.search(r'\bремонт квартир\b', name) or re.search(r'\bРемонт офисов\b', name) or re.search(r'\bремонт офисов\b', name)\
                or re.search(r'\bОтделка помещений\b', name) or re.search(r'\bотделка помещений\b', name) or re.search(r'\bФасадные работы\b', name)\
                or re.search(r'\bфасадные работы\b', name) or re.search(r'\bБлагоустройство территорий\b', name) or re.search(r'\bблагоустройство территорий\b', name)\
                or re.search(r'\bБуровые работы\b', name) or re.search(r'\bбуровые работы\b', name) or re.search(r'\bВысотные работы\b', name)\
                or re.search(r'\bвысотные работы\b', name) or re.search(r'\bСтроительство домов\b', name) or re.search(r'\bстроительство домов\b', name)\
                or re.search(r'\bПромышленное строительство\b', name) or re.search(r'\bпромышленное строительство\b', name) or re.search(r'\bСварочные работы\b', name)\
                or re.search(r'\bсварочные работы\b', name) or re.search(r'\bПрокладка кабелей\b', name) or re.search(r'\bпрокладка кабелей\b', name)\
                or re.search(r'\bОтделка офисов\b', name) or re.search(r'\bотделка офисов\b', name):
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
                data = [(name, mail_2, city, number, operator, url, descr)]
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
    if html != None:
        get_page_data(html, url)




if __name__ == '__main__':
    multiprocessing.freeze_support()
    url_data = []
    
    
    with open("ru_domains_base.txt", "r") as f:
        for line in f:
            url = 'https://' + line.lower().strip()
            url_data.append(url)
        

        with Pool(3) as p:
            p.map_async(make_all, url_data, callback=end_func)
            p.close()
            p.join()
