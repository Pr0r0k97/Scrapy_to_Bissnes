import time
import requests
from bs4 import BeautifulSoup
import sqlite3
import re
import SiteMap
from multiprocessing import Pool

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
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


def get_page_data(html):
    hrefs_data = []
    soup = BeautifulSoup(html, 'lxml')
    ads = soup.find('div', class_='col-sm-8 col-sm-pull-4').find_all('div', class_='result result-default duckduckgo')
    for ad in ads:
        name = ad.find('a').text.replace("...", "")
        description = ad.find('p', class_='result-content').text.replace("...", "")
        hrefs = ad.find('a').get('href')
        hrefs_data.append(hrefs)
        for hre in hrefs_data:
            try:
                r = requests.get(hre, headers=headers)
                soups = BeautifulSoup(r.text, 'lxml')
                try:
                    mail = soups.find(string=re.compile('\w+\@\w+.\w+')).text.strip()
                    if len(mail) < 20:
                        mail_2 = mail
                except:
                    mail = ''
                try:
                    number = soups.find('a', string=re.compile('(\+7|8).\D*\d{3}\D*\d{3}\D*\d{2}\D*\d{2}')).text
                except:
                    number = ''
            except Exception as ex:
                continue
        data = [(name, mail_2, number, hrefs, description)]
        print(data)
        #insert_db(data)


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
    url = 'https://searx.org/search?q=%22%D0%A1%D1%82%D1%80%D0%BE%D0%B8%D1%82%D0%B5%D0%BB%D1%8C%D0%BD%D0%B0%D1%8F%20%D0%BA%D0%BE%D0%BC%D0%BF%D0%B0%D0%BD%D0%B8%D1%8F%22%20&categories=general&pageno=3&language=ru-RU'
    text = re.sub(r'pageno=\d+', 'pageno={}', url)
    urls = [text.format(str(i)) for i in range(2, 54)]

    # Подключаем мультипроцессинг
    with Pool(2) as p:
        p.map_async(make_all, urls, callback=end_func)
        p.close()
        p.join()


if __name__ == '__main__':
    main()
