import multiprocessing
import requests
from bs4 import BeautifulSoup
import sqlite3
import re
from multiprocessing import Pool
import colorama
import tqdm
import logging



logging.basicConfig(
    filename='HISTORYlistener.log',
    level=logging.DEBUG,
    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

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
        r = session.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            #print(f'Выполняется парсинг данной страницы {url}')
            return r.text
    except requests.exceptions.HTTPError as errh:
         pass
    except requests.exceptions.ConnectionError as errc:
        pass
    except requests.exceptions.Timeout as errt:
         pass
    except requests.exceptions.RequestException as err:
         pass
    except Exception as qwe:
         pass




def scan_tel(arg):
    try:
        ar = arg.replace(" ", '').replace("-", '').replace("(", '').replace(")", '')
        url = "http://num.voxlink.ru/get/"
        querystring = {"num": f"{ar}"}
        payload = ""
        response = requests.request("GET", url, data=payload, params=querystring)
        return response.json()
    except Exception:
        pass


def get_page_data(html, url):
    global mail_2, number, city, operator, number_one, descr
    if html != None:
        try:
            soup = BeautifulSoup(html, 'lxml')
        except:
            soup = ''    
        try:
            name = soup.find('title').text
        except:
            name = ''
        try:
            descriptions = soup.find('head').find_all(attrs={"name": "description"})
        except:
            descriptions = ''
        for descript in descriptions:
            try:
                descr = descript.get('content') 
            except:
                descr = ''
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
                insert_db(data)
        else:
            pass

def get_page_data_internet_magaz(html, url):
    global mail_2, number, city, operator, number_one, descr
    if html != None:
        try:
            soup = BeautifulSoup(html, 'lxml')
        except:
            soup = ''
        try:
            name = soup.find('title').text
        except:
            name = ''
        try:
            descriptions = soup.find('head').find_all(attrs={"name": "description"})
        except:
            descriptions = ''
        for descript in descriptions:
            try:
                descr = descript.get('content') 
            except:
                descr = ''
        if re.search(r'\bИнтернет магазин\b', name) or re.search(r'\bинтернет магазин\b', name):
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
                insert_db_internet_magaz(data)
        else:
            pass

def get_page_data_med_centr(html, url):
    global mail_2, number, city, operator, number_one, descr
    if html != None:
        try:
            soup = BeautifulSoup(html, 'lxml')
        except:
            soup = ''
        try:
            name = soup.find('title').text
        except:
            name = ''
        try:
            descriptions = soup.find('head').find_all(attrs={"name": "description"})
        except:
            descriptions = ''
        for descript in descriptions:
            try:
                descr = descript.get('content') 
            except:
                descr = ''
        if re.search(r'\bМедицинский центр\b', name) or re.search(r'\bмедицинский центр\b', name):
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
                insert_db_med_centr(data)
        else:
            pass

def get_page_data_opt_company(html, url):
    global mail_2, number, city, operator, number_one, descr
    if html != None:
        try:
            soup = BeautifulSoup(html, 'lxml')
        except:
            soup = ''
        try:
            name = soup.find('title').text
        except:
            name = ''
        try:
            descriptions = soup.find('head').find_all(attrs={"name": "description"})
        except:
            descriptions = ''
        for descript in descriptions:
            try:
                descr = descript.get('content') 
            except:
                descr = ''    
        if re.search(r'\bДистрибутор\b', name) or re.search(r'\bдистрибутор\b', name) or re.search(r'\bДистрибьютор\b', name)\
            or re.search(r'\bдистрибьютор\b', name) or re.search(r'\bПоставщик\b', name) or re.search(r'\bпоставщик\b', name)\
            or re.search(r'\bОптовая компания\b', name) or re.search(r'\bоптовая компания\b', name) or re.search(r'\bТовары оптом\b', name)\
            or re.search(r'\bтовары оптом\b', name) or re.search(r'\bИмпортер\b', name) or re.search(r'\bимпортер\b', name) or re.search(r'\bОптовик\b', name)\
            or re.search(r'\bоптовик\b', name) or re.search(r'\bОптовая торговля\b', name) or re.search(r'\bоптовая торговля\b', name) or re.search(r'\bТорговля оптом\b', name)\
            or re.search(r'\bторговля оптом\b', name):
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
                insert_db_opt_company(data)
        else:
            pass

def get_page_data_stomatolog(html, url):
    global mail_2, number, city, operator, number_one, descr
    if html != None:
        try:
            soup = BeautifulSoup(html, 'lxml')
        except:
            soup = ''
        try:
            name = soup.find('title').text
        except:
            name = ''
        try:
            descriptions = soup.find('head').find_all(attrs={"name": "description"})
        except:
            descriptions = ''
        for descript in descriptions:
            try:
                descr = descript.get('content') 
            except:
                descr = ''    
        if re.search(r'\bСтоматология\b', name) or re.search(r'\bстоматология\b', name):
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
                insert_db_stomatolog(data)
        else:
            pass

def get_page_data_proizvodstvo_kompany(html, url):
    global mail_2, number, city, operator, number_one, descr
    if html != None:
        try:
            soup = BeautifulSoup(html, 'lxml')
        except:
            soup = ''
        try:
            name = soup.find('title').text
        except:
            name = ''
        try:
            descriptions = soup.find('head').find_all(attrs={"name": "description"})
        except:
            descriptions = ''
        for descript in descriptions:
            try:
                descr = descript.get('content') 
            except:
                descr = ''    
        if re.search(r'\bПроизводство\b', name) or re.search(r'\bпроизводство\b', name) or re.search(r'\bПроизводитель\b', name)\
            or re.search(r'\bпроизводитель\b', name):
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
                insert_db_proizvodstvo_kompany(data)
        else:
            pass

def get_page_data_avtoServis(html, url):
    global mail_2, number, city, operator, number_one, descr
    if html != None:
        try:
            soup = BeautifulSoup(html, 'lxml')
        except:
            soup = ''
        try:
            name = soup.find('title').text
        except:
            name = ''
        try:
            descriptions = soup.find('head').find_all(attrs={"name": "description"})
        except:
            descriptions = ''
        for descript in descriptions:
            try:
                descr = descript.get('content') 
            except:
                descr = ''    
        if re.search(r'\bАвтосервис\b', name) or re.search(r'\bавтосервис\b', name) or re.search(r'\bКузовной ремонт\b', name)\
            or re.search(r'\bкузовной ремонт\b', name):
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
                insert_db_avtoServis(data)
        else:
            pass

def get_page_Agent_nedvizhimosti(html, url):
    global mail_2, number, city, operator, number_one, descr
    if html != None:
        try:
            soup = BeautifulSoup(html, 'lxml')
        except:
            soup = ''
        try:
            name = soup.find('title').text
        except:
            name = ''
        try:
            descriptions = soup.find('head').find_all(attrs={"name": "description"})
        except:
            descriptions = ''
        for descript in descriptions:
            try:
                descr = descript.get('content') 
            except:
                descr = ''    
        if re.search(r'\bАгентство недвижимости\b', name) or re.search(r'\bагентство недвижимости\b', name) or re.search(r'\bКупить квартиру\b', name)\
            or re.search(r'\bкупить квартиру\b', name):
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
                insert_db_Agent_nedvizhimosti(data)
        else:
            pass

def get_page_ChinoMontaz(html, url):
    global mail_2, number, city, operator, number_one, descr
    if html != None:
        try:
            soup = BeautifulSoup(html, 'lxml')
        except:
            soup = ''
        try:
            name = soup.find('title').text
        except:
            name = ''
        try:
            descriptions = soup.find('head').find_all(attrs={"name": "description"})
        except:
            descriptions = ''
        for descript in descriptions:
            try:
                descr = descript.get('content') 
            except:
                descr = ''
        if re.search(r'\bШиномонтаж\b', name) or re.search(r'\bшиномонтаж\b', name) or re.search(r'\bРемонт колес\b', name)\
            or re.search(r'\bремонт колес\b', name):
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
                insert_db_ChinoMontaz(data)
        else:
            pass

def get_page_remont_kvartir(html, url):
    global mail_2, number, city, operator, number_one, descr
    if html != None:
        try:
            soup = BeautifulSoup(html, 'lxml')
        except:
            soup = ''
        try:
            name = soup.find('title').text
        except:
            name = ''
        try:
            descriptions = soup.find('head').find_all(attrs={"name": "description"})
        except:
            descriptions = ''
        for descript in descriptions:
            try:
                descr = descript.get('content') 
            except:
                descr = ''   
        if re.search(r'\bОтделка квартир\b', name) or re.search(r'\bотделка квартир\b', name) or re.search(r'\bОтделка офисов\b', name)\
            or re.search(r'\bотделка офисов\b', name) or re.search(r'\bРемонт квартир\b', name) or re.search(r'\bремонт квартир\b', name)\
                or re.search(r'\bРемонт офисов\b', name) or re.search(r'\bРемонт офисов\b', name):
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
                insert_db_remont_kvartir(data)
        else:
            pass

def get_page_tur_agenstvo(html, url):
    global mail_2, number, city, operator, number_one, descr
    if html != None:
        try:
            soup = BeautifulSoup(html, 'lxml')
        except:
            soup = ''
        try:
            name = soup.find('title').text
        except:
            name = ''
        try:
            descriptions = soup.find('head').find_all(attrs={"name": "description"})
        except:
            descriptions = ''
        for descript in descriptions:
            try:
                descr = descript.get('content') 
            except:
                descr = ''    
        if re.search(r'\bТуристическое агентство\b', name) or re.search(r'\bтуристическое агентство\b', name) or re.search(r'\bГорящие туры\b', name)\
            or re.search(r'\bгорящие туры\b', name) or re.search(r'\bКупить тур\b', name) or re.search(r'\bкупить тур\b', name)\
            or re.search(r'\bКупить путевку\b', name) or re.search(r'\bкупить путевку\b', name)\
            or re.search(r'\bтур онлайн\b', name) or re.search(r'\bтур онлайн\b', name):
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
                insert_db_tur_agenstvo(data)
        else:
            pass

def get_page_salon_krasot(html, url):
    global mail_2, number, city, operator, number_one, descr
    if html != None:
        try:
            soup = BeautifulSoup(html, 'lxml')
        except:
            soup = ''
        try:
            name = soup.find('title').text
        except:
            name = ''
        try:
            descriptions = soup.find('head').find_all(attrs={"name": "description"})
        except:
            descriptions = ''
        for descript in descriptions:
            try:
                descr = descript.get('content') 
            except:
                descr = ''    
        if re.search(r'\bСалон красоты\b', name) or re.search(r'\bсалон красоты\b', name) or re.search(r'\bКосметология\b', name)\
            or re.search(r'\bкосметология\b', name) or re.search(r'\bКосметолог\b', name) or re.search(r'\bкосметолог\b', name)\
            or re.search(r'\bSPA салон\b', name) or re.search(r'\bСтудия маникюра\b', name)\
            or re.search(r'\bстудия маникюра\b', name) or re.search(r'\bСтудия педикюра\b', name) or re.search(r'\bСтудия педикюра\b', name)\
            or re.search(r'\bстудия педикюра\b', name) or re.search(r'\bСтудия красоты\b', name) or re.search(r'\bстудия красоты\b', name):
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
                insert_db_salon_krasot(data)
        else:
            pass

def get_page_restoran(html, url):
    global mail_2, number, city, operator, number_one, descr
    if html != None:
        try:
            soup = BeautifulSoup(html, 'lxml')
        except:
            soup = ''
        try:
            name = soup.find('title').text
        except:
            name = ''
        try:
            descriptions = soup.find('head').find_all(attrs={"name": "description"})
        except:
            descriptions = ''
        for descript in descriptions:
            try:
                descr = descript.get('content') 
            except:
                descr = ''    
        if re.search(r'\bРесторан\b', name) or re.search(r'\bресторан\b', name) or re.search(r'\bКафе\b', name)\
            or re.search(r'\bкафе\b', name):
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
                insert_db_restoran(data)
        else:
            pass

def get_page_transport_kompany(html, url):
    global mail_2, number, city, operator, number_one, descr
    if html != None:
        try:
            soup = BeautifulSoup(html, 'lxml')
        except:
            soup = ''
        try:
            name = soup.find('title').text
        except:
            name = ''
        try:
            descriptions = soup.find('head').find_all(attrs={"name": "description"})
        except:
            descriptions = ''
        for descript in descriptions:
            try:
                descr = descript.get('content') 
            except:
                descr = ''    
        if re.search(r'\bГрузоперевозки\b', name) or re.search(r'\bгрузоперевозки\b', name) or re.search(r'\bТранспортная компания\b', name)\
            or re.search(r'\bтранспортная компания\b', name) or re.search(r'\bДоставка грузов\b', name) or re.search(r'\bдоставка грузов\b', name)\
            or re.search(r'\bТранспортные услуги\b', name) or re.search(r'\bтранспортные услуги\b', name) or re.search(r'\bМеждународные перевозки\b', name)\
            or re.search(r'\bмеждународные перевозки\b', name):
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
                insert_db_transport_kompany(data)
        else:
            pass

def get_page_torgov_centr(html, url):
    global mail_2, number, city, operator, number_one, descr
    if html != None:
        try:
            soup = BeautifulSoup(html, 'lxml')
        except:
            soup = ''
        try:
            name = soup.find('title').text
        except:
            name = ''
        try:
            descriptions = soup.find('head').find_all(attrs={"name": "description"})
        except:
            descriptions = ''
        for descript in descriptions:
            try:
                descr = descript.get('content') 
            except:
                descr = ''    
        if re.search(r'\bТорговый центр\b', name) or re.search(r'\bторговый центр\b', name) or re.search(r'\bТоргово развлекательный центр\b', name)\
            or re.search(r'\bторгово развлекательный центр\b', name) or re.search(r'\bРазвлекательный центр\b', name) or re.search(r'\bразвлекательный центр\b', name)\
            or re.search(r'\bТорговый центр\b', name) or re.search(r'\bторговый центр\b', name):
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
                insert_db_torgov_centr(data)
        else:
            pass

def get_page_otel_gost(html, url):
    global mail_2, number, city, operator, number_one, descr
    if html != None:
        try:
            soup = BeautifulSoup(html, 'lxml')
        except:
            soup = ''
        try:
            name = soup.find('title').text
        except:
            name = ''
        try:
            descriptions = soup.find('head').find_all(attrs={"name": "description"})
        except:
            descriptions = ''
        for descript in descriptions:
            try:
                descr = descript.get('content') 
            except:
                descr = ''    
        if re.search(r'\bГостиница\b', name) or re.search(r'\bгостиница\b', name) or re.search(r'\bОтель\b', name)\
            or re.search(r'\bотель\b', name):
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
                insert_db_otel_gost(data)
        else:
            pass

def get_page_urist_advokat(html, url):
    global mail_2, number, city, operator, number_one, descr
    if html != None:
        try:
            soup = BeautifulSoup(html, 'lxml')
        except:
            soup = ''
        try:
            name = soup.find('title').text
        except:
            name = ''
        try:
            descriptions = soup.find('head').find_all(attrs={"name": "description"})
        except:
            descriptions = ''
        for descript in descriptions:
            try:
                descr = descript.get('content') 
            except:
                descr = ''  
        if re.search(r'\bЮрист\b', name) or re.search(r'\bюрист\b', name) or re.search(r'\bАдвокат\b', name)\
            or re.search(r'\bадвокат\b', name) or re.search(r'\bЮридическая консультация\b', name) or re.search(r'\bюридическая консультация\b', name)\
            or re.search(r'\bЮридические услуги\b', name) or re.search(r'\bюридические услуги\b', name) or re.search(r'\bЮридическая помощь\b', name)\
            or re.search(r'\bюридическая помощь\b', name):
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
                insert_db_urist_advokat(data)
        else:
            pass

def get_page_apteki(html, url):
    global mail_2, number, city, operator, number_one, descr
    if html != None:
        try:
            soup = BeautifulSoup(html, 'lxml')
        except:
            soup = ''
        try:
            name = soup.find('title').text
        except:
            name = ''
        try:
            descriptions = soup.find('head').find_all(attrs={"name": "description"})
        except:
            descriptions = ''
        for descript in descriptions:
            try:
                descr = descript.get('content') 
            except:
                descr = '' 
        if re.search(r'\bАптека\b', name) or re.search(r'\bаптека\b', name):
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
                insert_db_apteki(data)
        else:
            pass

def get_page_reklam_agenstvo(html, url):
    global mail_2, number, city, operator, number_one, descr
    if html != None:
        try:
            soup = BeautifulSoup(html, 'lxml')
        except:
            soup = ''
        try:
            name = soup.find('title').text
        except:
            name = ''
        try:
            descriptions = soup.find('head').find_all(attrs={"name": "description"})
        except:
            descriptions = ''
        for descript in descriptions:
            try:
                descr = descript.get('content') 
            except:
                descr = ''
        if re.search(r'\bРекламное агенство\b', name) or re.search(r'\bрекламное агенство\b', name) or re.search(r'\bРеклама яндекс директ\b', name)\
             or re.search(r'\bреклама яндекс директ\b', name) or re.search(r'\bКонтекстная реклама\b', name) or re.search(r'\bконтекстная реклама\b', name)\
             or re.search(r'\bПродвижение в интернете\b', name) or re.search(r'\bпродвижение в интернете\b', name) or re.search(r'\bЯндекс директ\b', name)\
             or re.search(r'\bяндекс директ\b', name) or re.search(r'\bРеклама в интернете\b', name) or re.search(r'\bреклама в интернете\b', name)\
             or re.search(r'\bПродвижение сайтов\b', name) or re.search(r'\bпродвижение сайтов\b', name)\
             or re.search(r'\bБаннерная реклама\b', name) or re.search(r'\bбаннерная реклама\b', name) or re.search(r'\bОнлайн маркетинг\b', name)\
             or re.search(r'\bонлайн маркетинг\b', name):
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
                insert_db_reklam_agenstvo(data)
        else:
            pass

def get_page_scklad_komleks(html, url):
    global mail_2, number, city, operator, number_one, descr
    if html != None:
        try:
            soup = BeautifulSoup(html, 'lxml')
        except:
            soup = ''
        try:
            name = soup.find('title').text
        except:
            name = ''
        try:
            descriptions = soup.find('head').find_all(attrs={"name": "description"})
        except:
            descriptions = ''
        for descript in descriptions:
            try:
                descr = descript.get('content') 
            except:
                descr = ''    
        if re.search(r'\bСкладской комплекс\b', name) or re.search(r'\bскладской комплекс\b', name) or re.search(r'\bСклад\b', name)\
             or re.search(r'\bсклад\b', name) or re.search(r'\bЛогистический комплекс\b', name) or re.search(r'\bлогистический комплекс\b', name)\
             or re.search(r'\bАренда склада\b', name) or re.search(r'\bаренда склада\b', name) or re.search(r'\bСклады в аренду\b', name)\
             or re.search(r'\bсклады в аренду\b', name):
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
                insert_db_scklad_komleks(data)
        else:
            pass

def get_page_veterenar(html, url):
    global mail_2, number, city, operator, number_one, descr
    if html != None:
        try:
            soup = BeautifulSoup(html, 'lxml')
        except:
            soup = ''
        try:
            name = soup.find('title').text
        except:
            name = ''
        try:
            descriptions = soup.find('head').find_all(attrs={"name": "description"})
        except:
            descriptions = ''
        for descript in descriptions:
            try:
                descr = descript.get('content') 
            except:
                descr = ''    
        if re.search(r'\bВетеринарная клиника\b', name) or re.search(r'\bветеринарная клиника\b', name) or re.search(r'\bЛечение животных\b', name)\
             or re.search(r'\bлечение животных\b', name) or re.search(r'\bВетеринарные услуги\b', name) or re.search(r'\bветеринарные услуги\b', name)\
             or re.search(r'\bВетеринарная клиника\b', name) or re.search(r'\bветеринарная клиника\b', name):
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
                insert_db_veterenar(data)
        else:
            pass

def get_page_notarius(html, url):
    global mail_2, number, city, operator, number_one, descr
    if html != None:
        try:
            soup = BeautifulSoup(html, 'lxml')
        except:
            soup = ''
        try:
            name = soup.find('title').text
        except:
            name = ''
        try:
            descriptions = soup.find('head').find_all(attrs={"name": "description"})
        except:
            descriptions = ''
        for descript in descriptions:
            try:
                descr = descript.get('content') 
            except:
                descr = ''    
        if re.search(r'\bНотариус\b', name) or re.search(r'\bнотариус\b', name) or re.search(r'\bНотариальные услуги\b', name)\
             or re.search(r'\bнотариальные услуги\b', name) or re.search(r'\bНотариальная контора\b', name) or re.search(r'\bнотариальная контора\b', name):
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
                insert_db_notarius(data)
        else:
            pass

def get_page_proizvoditeli(html, url):
    global mail_2, number, city, operator, number_one, descr
    if html != None:
        try:
            soup = BeautifulSoup(html, 'lxml')
        except:
            soup = ''
        try:
            name = soup.find('title').text
        except:
            name = ''
        try:
            descriptions = soup.find('head').find_all(attrs={"name": "description"})
        except:
            descriptions = ''
        for descript in descriptions:
            try:
                descr = descript.get('content') 
            except:
                descr = ''
        if re.search(r'\bПроизводство\b', name) or re.search(r'\bпроизводство\b', name) or re.search(r'\bПроизводитель\b', name)\
             or re.search(r'\bпроизводитель\b', name):
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
                insert_db_proizvoditelit(data)
        else:
            pass



def insert_db_proizvoditelit(data):
    connection = sqlite3.connect("mydatabase_Stoit_Kompani.db")
    cursor = connection.cursor()
    urls = [x[5] for x in data]
    for s in urls:
        pass
    cursor.execute(f"SELECT hrefs FROM pars_proizvoditelit WHERE hrefs = '{s}'")
    if cursor.fetchone() is None:
        cursor.executemany("INSERT INTO pars_proizvoditelit VALUES (?,?,?,?,?,?,?)", data)
        connection.commit()
        print(f"{RESET}Запись Добавлена ")
    else:
        print(f"{RESET}Уже есть")

def insert_db_urist_advokat(data):
    connection = sqlite3.connect("mydatabase_Stoit_Kompani.db")
    cursor = connection.cursor()
    urls = [x[5] for x in data]
    for s in urls:
        pass
    cursor.execute(f"SELECT hrefs FROM pars_urist_advokat WHERE hrefs = '{s}'")
    if cursor.fetchone() is None:
        cursor.executemany("INSERT INTO pars_urist_advokat VALUES (?,?,?,?,?,?,?)", data)
        connection.commit()
        print(f"{RESET}Запись Добавлена ")
    else:
        print(f"{RESET}Уже есть")

def insert_db_apteki(data):
    connection = sqlite3.connect("mydatabase_Stoit_Kompani.db")
    cursor = connection.cursor()
    urls = [x[5] for x in data]
    for s in urls:
        pass
    cursor.execute(f"SELECT hrefs FROM pars_apteki WHERE hrefs = '{s}'")
    if cursor.fetchone() is None:
        cursor.executemany("INSERT INTO pars_apteki VALUES (?,?,?,?,?,?,?)", data)
        connection.commit()
        print(f"{RESET}Запись Добавлена ")
    else:
        print(f"{RESET}Уже есть")

def insert_db_scklad_komleks(data):
    connection = sqlite3.connect("mydatabase_Stoit_Kompani.db")
    cursor = connection.cursor()
    urls = [x[5] for x in data]
    for s in urls:
        pass
    cursor.execute(f"SELECT hrefs FROM pars_scklad_komleks WHERE hrefs = '{s}'")
    if cursor.fetchone() is None:
        cursor.executemany("INSERT INTO pars_scklad_komleks VALUES (?,?,?,?,?,?,?)", data)
        connection.commit()
        print(f"{RESET}Запись Добавлена ")
    else:
        print(f"{RESET}Уже есть")

def insert_db_veterenar(data):
    connection = sqlite3.connect("mydatabase_Stoit_Kompani.db")
    cursor = connection.cursor()
    urls = [x[5] for x in data]
    for s in urls:
        pass
    cursor.execute(f"SELECT hrefs FROM pars_veterenar WHERE hrefs = '{s}'")
    if cursor.fetchone() is None:
        cursor.executemany("INSERT INTO pars_veterenar VALUES (?,?,?,?,?,?,?)", data)
        connection.commit()
        print(f"{RESET}Запись Добавлена ")
    else:
        print(f"{RESET}Уже есть")

def insert_db_reklam_agenstvo(data):
    connection = sqlite3.connect("mydatabase_Stoit_Kompani.db")
    cursor = connection.cursor()
    urls = [x[5] for x in data]
    for s in urls:
        pass
    cursor.execute(f"SELECT hrefs FROM pars_reklam_agenstvo WHERE hrefs = '{s}'")
    if cursor.fetchone() is None:
        cursor.executemany("INSERT INTO pars_reklam_agenstvo VALUES (?,?,?,?,?,?,?)", data)
        connection.commit()
        print(f"{RESET}Запись Добавлена ")
    else:
        print(f"{RESET}Уже есть")

def insert_db_notarius(data):
    connection = sqlite3.connect("mydatabase_Stoit_Kompani.db")
    cursor = connection.cursor()
    urls = [x[5] for x in data]
    for s in urls:
        pass
    cursor.execute(f"SELECT hrefs FROM pars_notarius WHERE hrefs = '{s}'")
    if cursor.fetchone() is None:
        cursor.executemany("INSERT INTO pars_notarius VALUES (?,?,?,?,?,?,?)", data)
        connection.commit()
        print(f"{RESET}Запись Добавлена ")
    else:
        print(f"{RESET}Уже есть")

def insert_db_otel_gost(data):
    connection = sqlite3.connect("mydatabase_Stoit_Kompani.db")
    cursor = connection.cursor()
    urls = [x[5] for x in data]
    for s in urls:
        pass
    cursor.execute(f"SELECT hrefs FROM pars_otel_gost WHERE hrefs = '{s}'")
    if cursor.fetchone() is None:
        cursor.executemany("INSERT INTO pars_otel_gost VALUES (?,?,?,?,?,?,?)", data)
        connection.commit()
        print(f"{RESET}Запись Добавлена ")
    else:
        print(f"{RESET}Уже есть")

def insert_db_torgov_centr(data):
    connection = sqlite3.connect("mydatabase_Stoit_Kompani.db")
    cursor = connection.cursor()
    urls = [x[5] for x in data]
    for s in urls:
        pass
    cursor.execute(f"SELECT hrefs FROM pars_torgov_centr WHERE hrefs = '{s}'")
    if cursor.fetchone() is None:
        cursor.executemany("INSERT INTO pars_torgov_centr VALUES (?,?,?,?,?,?,?)", data)
        connection.commit()
        print(f"{RESET}Запись Добавлена ")
    else:
        print(f"{RESET}Уже есть")

def insert_db_transport_kompany(data):
    connection = sqlite3.connect("mydatabase_Stoit_Kompani.db")
    cursor = connection.cursor()
    urls = [x[5] for x in data]
    for s in urls:
        pass
    cursor.execute(f"SELECT hrefs FROM pars_transport_kompany WHERE hrefs = '{s}'")
    if cursor.fetchone() is None:
        cursor.executemany("INSERT INTO pars_transport_kompany VALUES (?,?,?,?,?,?,?)", data)
        connection.commit()
        print(f"{RESET}Запись Добавлена ")
    else:
        print(f"{RESET}Уже есть")

def insert_db_restoran(data):
    connection = sqlite3.connect("mydatabase_Stoit_Kompani.db")
    cursor = connection.cursor()
    urls = [x[5] for x in data]
    for s in urls:
        pass
    cursor.execute(f"SELECT hrefs FROM pars_restoran WHERE hrefs = '{s}'")
    if cursor.fetchone() is None:
        cursor.executemany("INSERT INTO pars_restoran VALUES (?,?,?,?,?,?,?)", data)
        connection.commit()
        print(f"{RESET}Запись Добавлена ")
    else:
        print(f"{RESET}Уже есть")

def insert_db_salon_krasot(data):
    connection = sqlite3.connect("mydatabase_Stoit_Kompani.db")
    cursor = connection.cursor()
    urls = [x[5] for x in data]
    for s in urls:
        pass
    cursor.execute(f"SELECT hrefs FROM pars_salon_krasot WHERE hrefs = '{s}'")
    if cursor.fetchone() is None:
        cursor.executemany("INSERT INTO pars_salon_krasot VALUES (?,?,?,?,?,?,?)", data)
        connection.commit()
        print(f"{RESET}Запись Добавлена ")
    else:
        print(f"{RESET}Уже есть")

def insert_db_tur_agenstvo(data):
    connection = sqlite3.connect("mydatabase_Stoit_Kompani.db")
    cursor = connection.cursor()
    urls = [x[5] for x in data]
    for s in urls:
        pass
    cursor.execute(f"SELECT hrefs FROM pars_tur_agenstvo WHERE hrefs = '{s}'")
    if cursor.fetchone() is None:
        cursor.executemany("INSERT INTO pars_tur_agenstvo VALUES (?,?,?,?,?,?,?)", data)
        connection.commit()
        print(f"{RESET}Запись Добавлена ")
    else:
        print(f"{RESET}Уже есть")

def insert_db_remont_kvartir(data):
    connection = sqlite3.connect("mydatabase_Stoit_Kompani.db")
    cursor = connection.cursor()
    urls = [x[5] for x in data]
    for s in urls:
        pass
    cursor.execute(f"SELECT hrefs FROM pars_remont_kvartir WHERE hrefs = '{s}'")
    if cursor.fetchone() is None:
        cursor.executemany("INSERT INTO pars_remont_kvartir VALUES (?,?,?,?,?,?,?)", data)
        connection.commit()
        print(f"{RESET}Запись Добавлена ")
    else:
        print(f"{RESET}Уже есть")

def insert_db_ChinoMontaz(data):
    connection = sqlite3.connect("mydatabase_Stoit_Kompani.db")
    cursor = connection.cursor()
    urls = [x[5] for x in data]
    for s in urls:
        pass
    cursor.execute(f"SELECT hrefs FROM pars_ChinoMontaz WHERE hrefs = '{s}'")
    if cursor.fetchone() is None:
        cursor.executemany("INSERT INTO pars_ChinoMontaz VALUES (?,?,?,?,?,?,?)", data)
        connection.commit()
        print(f"{RESET}Запись Добавлена ")
    else:
        print(f"{RESET}Уже есть")

def insert_db_Agent_nedvizhimosti(data):
    connection = sqlite3.connect("mydatabase_Stoit_Kompani.db")
    cursor = connection.cursor()
    urls = [x[5] for x in data]
    for s in urls:
        pass
    cursor.execute(f"SELECT hrefs FROM pars_Agent_nedvizhimosti WHERE hrefs = '{s}'")
    if cursor.fetchone() is None:
        cursor.executemany("INSERT INTO pars_Agent_nedvizhimosti VALUES (?,?,?,?,?,?,?)", data)
        connection.commit()
        print(f"{RESET}Запись Добавлена ")
    else:
        print(f"{RESET}Уже есть")

def insert_db_avtoServis(data):
    connection = sqlite3.connect("mydatabase_Stoit_Kompani.db")
    cursor = connection.cursor()
    urls = [x[5] for x in data]
    for s in urls:
        pass
    cursor.execute(f"SELECT hrefs FROM pars_avtoServis WHERE hrefs = '{s}'")
    if cursor.fetchone() is None:
        cursor.executemany("INSERT INTO pars_avtoServis VALUES (?,?,?,?,?,?,?)", data)
        connection.commit()
        print(f"{RESET}Запись Добавлена ")
    else:
        print(f"{RESET}Уже есть")

def insert_db_proizvodstvo_kompany(data):
    connection = sqlite3.connect("mydatabase_Stoit_Kompani.db")
    cursor = connection.cursor()
    urls = [x[5] for x in data]
    for s in urls:
        pass
    cursor.execute(f"SELECT hrefs FROM pars_proizvodstvo_kompany WHERE hrefs = '{s}'")
    if cursor.fetchone() is None:
        cursor.executemany("INSERT INTO pars_proizvodstvo_kompany VALUES (?,?,?,?,?,?,?)", data)
        connection.commit()
        print(f"{RESET}Запись Добавлена ")
    else:
        print(f"{RESET}Уже есть")

def insert_db_stomatolog(data):
    connection = sqlite3.connect("mydatabase_Stoit_Kompani.db")
    cursor = connection.cursor()
    urls = [x[5] for x in data]
    for s in urls:
        pass
    cursor.execute(f"SELECT hrefs FROM pars_stomatolog WHERE hrefs = '{s}'")
    if cursor.fetchone() is None:
        cursor.executemany("INSERT INTO pars_stomatolog VALUES (?,?,?,?,?,?,?)", data)
        connection.commit()
        print(f"{RESET}Запись Добавлена ")
    else:
        print(f"{RESET}Уже есть")

def insert_db_opt_company(data):
    connection = sqlite3.connect("mydatabase_Stoit_Kompani.db")
    cursor = connection.cursor()
    urls = [x[5] for x in data]
    for s in urls:
        pass
    cursor.execute(f"SELECT hrefs FROM pars_opt_company WHERE hrefs = '{s}'")
    if cursor.fetchone() is None:
        cursor.executemany("INSERT INTO pars_opt_company VALUES (?,?,?,?,?,?,?)", data)
        connection.commit()
        print(f"{RESET}Запись Добавлена ")
    else:
        print(f"{RESET}Уже есть")

def insert_db_med_centr(data):
    connection = sqlite3.connect("mydatabase_Stoit_Kompani.db")
    cursor = connection.cursor()
    urls = [x[5] for x in data]
    for s in urls:
        pass
    cursor.execute(f"SELECT hrefs FROM pars_med_centr WHERE hrefs = '{s}'")
    if cursor.fetchone() is None:
        cursor.executemany("INSERT INTO pars_med_centr VALUES (?,?,?,?,?,?,?)", data)
        connection.commit()
        print(f"{RESET}Запись Добавлена ")
    else:
        print(f"{RESET}Уже есть")

def insert_db_internet_magaz(data):
    connection = sqlite3.connect("mydatabase_Stoit_Kompani.db")
    cursor = connection.cursor()
    urls = [x[5] for x in data]
    for s in urls:
        pass
    cursor.execute(f"SELECT hrefs FROM pars_Internet_magaz WHERE hrefs = '{s}'")
    if cursor.fetchone() is None:
        cursor.executemany("INSERT INTO pars_Internet_magaz VALUES (?,?,?,?,?,?,?)", data)
        connection.commit()
        print(f"{RESET}Запись Добавлена ")
    else:
        print(f"{RESET}Уже есть")

def insert_db(data):
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
        try:
            get_page_data(html, url)
            get_page_data_internet_magaz(html, url)
            get_page_data_med_centr(html, url)
            get_page_data_opt_company(html, url)
            get_page_data_stomatolog(html, url)
            get_page_data_proizvodstvo_kompany(html, url)
            get_page_data_avtoServis(html, url)
            get_page_Agent_nedvizhimosti(html, url)
            get_page_ChinoMontaz(html, url)
            get_page_remont_kvartir(html, url)
            get_page_tur_agenstvo(html, url)
            get_page_salon_krasot(html, url)
            get_page_restoran(html, url)
            get_page_transport_kompany(html, url)
            get_page_torgov_centr(html, url)
            get_page_otel_gost(html, url)
            get_page_urist_advokat(html, url)
            get_page_apteki(html, url)
            get_page_reklam_agenstvo(html, url)
            get_page_scklad_komleks(html, url)
            get_page_veterenar(html, url)
            get_page_notarius(html, url)
            get_page_proizvoditeli(html, url)
        except:
            pass

        
if __name__ == '__main__':
    multiprocessing.freeze_support()
    url_data = []
    
    results = []
    with open("ru_domains_base.txt", "r") as f:
        for line in f:
            url = 'https://' + line.lower().strip()
            url_data.append(url)
        print(multiprocessing.cpu_count() * 3)
        
        with Pool(multiprocessing.cpu_count()) as p:
            try:
                for _ in tqdm.tqdm(p.imap_unordered(make_all, url_data[536356:4988374]), total=len(url_data[536356:4988374])):
                    pass
                p.close()
                p.join()
            except Exception as eee:
                logging.error(eee)
                print(eee)
    
