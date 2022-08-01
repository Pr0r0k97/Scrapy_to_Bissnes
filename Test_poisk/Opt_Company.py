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

# def scan_tel(arg):
#     try:
#         if re.search(r'\b(812)|(921)|(911)\b', arg):
#             return 'Питер'
#         elif re.search(r'\b(495)|(499)|(495)|(909)|(916)\b', arg):
#             return 'Москва'
#         elif re.search(r'\b(383)\b', arg):
#             print('Новосибирск')
#         elif re.search(r'\b(343)\b', arg):
#             print('Екатеренбург')
#         elif re.search(r'\b(831)\b', arg):
#             print('Нижний Новгород')
#         elif re.search(r'\b(846)\b', arg):
#             print('Самара')
#         elif re.search(r'\b(843)\b', arg):
#             print('Казань')
#         elif re.search(r'\b(381)\b', arg):
#             print('Омск')
#         elif re.search(r'\b(351)\b', arg):
#             print('Челябинск')
#         elif re.search(r'\b(863)\b', arg):
#             print('Ростов')
#         elif re.search(r'\b(347)\b', arg):
#             print('Уфа')
#         elif re.search(r'\b(342)\b', arg):
#             print('Пермь')
#         elif re.search(r'\b(844)\b', arg):
#             print('Волгоград ')
#         elif re.search(r'\b(391)\b', arg):
#             print('Красноярск')
#         elif re.search(r'\b(473)\b', arg):
#             print('Воронеж')
#         elif re.search(r'\b(845)\b', arg):
#             print('Саратов')
#         elif re.search(r'\b(848)\b', arg):
#             print('Тольятти')
#         elif re.search(r'\b(861)|(918)\b', arg):
#             return 'Краснодар'
#         elif re.search(r'\b(341)\b', arg):
#             print('Ижевск')
#         elif re.search(r'\b(485)\b', arg):
#             print('Ярославль ')
#         elif re.search(r'\b(800)\b', arg):
#             return 'Россия'
#         else:
#             return ''
#     except AttributeError:
#         pass
#

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
                                    if not re.search(r'\bРейтинг|рейтинг\b', name):
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
                                                            city = scan_tel(number)
                                                            #number_data.clear()
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
    url = 'https://nova.rambler.ru/search?utm_source=search&utm_campaign=self_promo&utm_medium=form&utm_content=search&query="строительная%20компания"&limitcontext=2&page=2'
    # with open('строительная компания.html', 'r', encoding='utf-8') as f:
    # get_page_data(get_html(url))
    text = re.sub(r'page=\d+', 'page={}', url)
    urls = [text.format(str(i)) for i in range(4, 7)]

    # Подключаем мультипроцессинг
    with Pool(2) as p:
        p.map_async(make_all, urls, callback=end_func)
        p.close()
        p.join()


if __name__ == '__main__':
    main()
