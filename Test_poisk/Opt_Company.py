import time
import requests
from bs4 import BeautifulSoup
import sqlite3
import re
from multiprocessing import Pool


headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
    "Cookes": 'sutm=search; ruid=HQAAALyP3WIBlc1RAVsmAQB=; SLO_G_WPT_TO=ru; _ym_uid=1658687423798917400; _ym_d=1658687423; SLO_GWPT_Show_Hide_tmp=1; SLO_wptGlobTipTmp=1; _ym_isad=2; _ym_visorc=b; bltsr=1; dvr=gAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA:1658687427; lvr=1658687427; hrwp=1; addruid=n1j6o5q8J68Um74Px87HY40T3K; adtech_uid=ab5e3e1d-d245-4d6f-a4b2-40794a4289cb:rambler.ru; adtech_uid=b4f20bf8-a45e-4426-9c6b-fec4f7ab5b93:nova.rambler.ru; rambler_3rdparty_v2=; lastgeoip=188.170.79.84; sts=0.1658687490.1.1658687490.2.1658687490.3.1658687490.4.1658687490; bltsr=1; geoid=85593; split-value=22.39; split-v2=5; rchainid={"message":"need session","code":-4000,"details":{"method":"/session/getRccid","requestId":"ridl5znxkq9qdnehr1ca"}}; nlv=1658677072'
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


def get_page_data(html):
    global mail_2, number
    hrefs_data = []
    soup = BeautifulSoup(html, 'lxml')
    ads = soup.find('div', class_='LayoutSearch__serp--3LMVS').find_all('article', class_='Serp__item--NO2th')
    for ad in ads:
        name = ad.find('h2', class_='Serp__title--3i6Ro').text.replace('...', '')
        description = ad.find('p', class_='Serp__snippet--2mmWu').text.replace('...', '')
        try:
            hrefs = ad.find('a', class_='Serp__link--S29wB undefined').get('href')
        except AttributeError:
            continue
        if not re.search(r'\bСтроительные компании\b', name):
            if not re.search(r'\bОтзывы\b', name):
                if not re.search(r'\bСписок\b', name):
                    if not re.search(r'\bКаталог\b', name):
                        if not re.search(r'\bТоп\b', name):
                            if not re.search(r'\bСтроительные\b', name):
                                if not re.search(r'\bЗастройщики\b', name):
                                    if not re.search(r'\bРейтинг\b', name):
                                        if not re.search(r'\bУслуги\b', name):
                                            if not re.search(r'\bЗаказы\b', name):
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
                                                            number = soups.find('a', string=re.compile('(\+7|8).\D*\d{3}\D*\d{3}\D*\d{2}\D*\d{2}')).text.strip()
                                                        except:
                                                            number = ''
                                                    except Exception as ex:
                                                        continue

                                                data = [(name, mail_2, number, hrefs, description)]
                                                print(data)
                                                #insert_db(data)
                                            else:
                                                pass




        # soups = BeautifulSoup(r.text,'lxml')
        # mail = soups.find(string=re.compile('\w+\@\w+.\w+'))
        # number = soups.find(string=re.compile('(\+7|8)\D*\d{3}\D*\d{3}\D*\d{2}\D*\d{2}'))
        # if mail != None:
        #     if len(mail) < 20:
        #         mail_data.append(mail)
        #     else:
        #         pass
        # else:
        #     pass
        # if number != None:
        #     if len(number) < 20:
        #         number_data.append(number)
        #     else:
        #         pass
        # else:
        #     pass
        # data = ([name, description, mail, number])
        # print(data)


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
    url = 'https://nova.rambler.ru/search?utm_source=search&utm_campaign=self_promo&utm_medium=form&utm_content=search&query="строительная%20компания"&limitcontext=2&page=2'
    #with open('строительная компания.html', 'r', encoding='utf-8') as f:
    #get_page_data(get_html(url))
    text = re.sub(r'page=\d+', 'page={}', url)
    urls = [text.format(str(i)) for i in range(2, 5)]

    # Подключаем мультипроцессинг
    with Pool(2) as p:
        p.map_async(make_all, urls, callback=end_func)
        p.close()
        p.join()


if __name__ == '__main__':
    main()
