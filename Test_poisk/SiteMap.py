import requests
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
import colorama
from requests.auth import HTTPProxyAuth
from fake_useragent import UserAgent
import time
ua = UserAgent()

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
}

# запускаем модуль colorama
colorama.init()

GREEN = colorama.Fore.GREEN
GRAY = colorama.Fore.LIGHTBLACK_EX
RESET = colorama.Fore.RESET
YELLOW = colorama.Fore.YELLOW

# инициализировать set's для внутренних и внешних ссылок (уникальные ссылки)
internal_urls = set()
external_urls = set()

total_urls_visited = 0


def is_valid(url):
    """
    Проверка url
    """
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)


def get_all_website_links(url):
    """
    Возвращает все найденные URL-адреса на `url, того же веб-сайта.
    """
    # все URL-адреса `url`
    urls = set()
    # доменное имя URL без протокола
    domain_name = urlparse(url).netloc
    soup = BeautifulSoup(requests.get(url, headers=headers).content, "html.parser")
    time.sleep(10)
    for a_tag in soup.findAll("a"):
        href = a_tag.attrs.get("href")
        if href == "" or href is None:
            # пустой тег href
            continue
        # присоединяемся к URL, если он относительный (не абсолютная ссылка)
        href = urljoin(url, href)
        parsed_href = urlparse(href)
        # удалить параметры URL GET, фрагменты URL и т. д.
        href = parsed_href.scheme + "://" + parsed_href.netloc + parsed_href.path
        if not is_valid(href):
            # неверный URL
            continue
        if href in internal_urls:
            # уже в наборе
            continue
        if domain_name not in href:
            # внешняя ссылка
            if href not in external_urls:
                print(f"{GRAY}[!] Внешняя ссылка: {href}{RESET}")
                external_urls.add(href)
            continue
        print(f"{GREEN}[*] Внутреннея ссылка: {href}{RESET}")
        urls.add(href)
        internal_urls.add(href)
    return urls


def crawl(url, max_urls=3):
    """
    Сканирует веб-страницу и извлекает все ссылки.
    Вы найдете все ссылки в глобальных переменных набора external_urls и internal_urls.
    параметры:
        max_urls (int): максимальное количество URL-адресов для сканирования, по умолчанию 30.
    """
    global total_urls_visited
    total_urls_visited += 1
    print(f"{YELLOW}[*] Проверено: {url}{RESET}")
    links = get_all_website_links(url)
    for link in links:
        if total_urls_visited > max_urls:
            break
        crawl(link, max_urls=max_urls)


if __name__ == "__main__":
    import argparse

    """
    parser = argparse.ArgumentParser(description="Link Extractor Tool with Python")
    parser.add_argument("url", help="The URL to extract links from.")
    parser.add_argument("-m", "--max-urls", help="Number of max URLs to crawl, default is 30.", default=30, type=int)

    args = parser.parse_args()
    url = args.url
    max_urls = args.max_urls
    """

    #url = 'https://www.cian.ru/'
    max_urls = 3

    crawl(url, max_urls=max_urls)

    print("[+] Total Internal links:", len(internal_urls))
    print("[+] Total External links:", len(external_urls))
    print("[+] Total URLs:", len(external_urls) + len(internal_urls))
    print("[+] Total crawled URLs:", max_urls)

    domain_name = urlparse(url).netloc

    # # сохранить внутренние ссылки в файле
    # with open(f"{domain_name}_internal_links.txt", "w") as f:
    #     for internal_link in internal_urls:
    #         print(internal_link.strip(), file=f)
    #
    # # сохранить внешние ссылки в файле
    # with open(f"{domain_name}_external_links.txt", "w") as f:
    #     for external_link in external_urls:
            #print(external_link.strip(), file=f)