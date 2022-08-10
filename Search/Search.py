import time
import requests
from bs4 import BeautifulSoup
import sqlite3
import re
from multiprocessing import Pool
import colorama
import asyncio
import aiohttp

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



async def get_html(url):  # Делаем запрос к странице
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as r:
                if r.ok:
                    print(f'{r.status} url: {url} ')
                    return url
    except Exception as e:
        pass


item = 0
async def get_page_data(html):
    global item
    if html != None:
        with open('200_ok.txt', 'a')as f:
            f.write(f"{html}\n")
            item += 1
            #print(f"{item} {html}")






# def end_func(response):
#     print("Задание завершено")

#
# def make_all(url):
#     html = get_html(url)
#     get_page_data(html)


async def main():
    url_data = []
    chank = 200
    tasks = []
    with open("ru_domains_base.txt", "r") as f:
        for line in f:
            url = 'https://' + line.lower().strip()
            task = asyncio.create_task(get_html(url))
            task1 = asyncio.create_task(get_page_data(task))
            tasks.append(task)
            tasks.append(task1)
            if len(tasks) == chank:
                await asyncio.gather(*tasks)
                tasks = []

        # # Подключаем мультипроцессинг
        # with Pool(20) as p:
        #     p.map_async(make_all, url_data, callback=end_func)
        #     p.close()
        #     p.join()


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
