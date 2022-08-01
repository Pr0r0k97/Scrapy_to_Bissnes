import time
import requests
from bs4 import BeautifulSoup
import sqlite3
import re
from multiprocessing import Pool
import random
import json


headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
    "Cookie": 'ruid=HQAAAIiF4mKOaWC/AQIQAAB=; SLO_G_WPT_TO=ru; SLO_GWPT_Show_Hide_tmp=1; SLO_wptGlobTipTmp=1; _ym_uid=165901249376373978; _ym_d=1659012493; addruid=d1N65t9xL01jE25d6r2N2Iu0t0; adtech_uid=9776acd8-9f40-49a2-b574-058e768b581e:rambler.ru; adtech_uid=a29228f5-0638-4b56-8074-3454b971d961:nova.rambler.ru; rambler_3rdparty_v2=; bltsr=1; rswitch=desktop; sort=3; split-value=16.51; split-v2=5; user-id_1.0.5_lr_lruid=pQ8AALGK4mLkxN++AUnSiAA=; c8980c62834072c480df58741f1fd039393df9aaea5446dbb1dd2187750209fe_2=HQAAAIiF4mKOaWC/AQIQAAB=; detect_count=1; pagelen=50; am=soft; sutm=search; geoid=54118936; _ym_isad=1; dvr=oAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA:1659193861; lastgeoip=188.170.76.89; sts=0.1659193882.1.1659012565.2.1659193882.3.1659012565.4.1659012565; _ym_visorc=b; nlv=1659184296; lvr=1659195105'
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
                        number = soups.find('a',
                                            string=re.compile('(\+7|8).\D*\d{3}\D*\d{3}\D*\d{2}\D*\d{2}')).text.strip()
                        city = scan_tel(number)
                        # number_data.clear()
                    except:
                        number = ''
                        city = ''
                except Exception as ex:
                    continue
            data = [(name, mail_2, city, number, hrefs, description)]
            print(data)
            # insert_db(data)
        else:
            pass




def insert_db(data):
    connection = sqlite3.connect('mydatabase_Stoit_Kompani.db')
    cursor = connection.cursor()
    urls = [x[2] for x in data]
    for s in urls:
        pass
    cursor.execute(f"SELECT hrefs FROM pars WHERE hrefs = '{s}'")
    if cursor.fetchone() is None:
        cursor.executemany("INSERT INTO pars VALUES (?,?,?,?,?)", data)
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
    numb = [3, 225, 17, 26, 40, 59, 52, 73, 102444, 10712, 10772, 10776, 10795, 10802, 10819, 10832, 10841]
    rand = random.choice(numb)
    #print(rand)
    url = f'http://xmlriver.com/search/xml?user=6019&key=10e92a3587615ff8137a89723928af6c1c4ef396&query=строительная%20компания&lr={rand}&page=21'
    # with open('строительная компания.html', 'r', encoding='utf-8') as f:
    # get_page_data(get_html(url))
    text = re.sub(r'page=\d+', 'page={}', url)
    urls = [text.format(str(i)) for i in range(4, 5)]

    # Подключаем мультипроцессинг
    with Pool(2) as p:
        p.map_async(make_all, urls, callback=end_func)
        p.close()
        p.join()


if __name__ == '__main__':
    main()
