import sqlite3
import asyncio
import pandas as pd
from requests_html import AsyncHTMLSession, HTMLSession
from fake_useragent import UserAgent


def get_pages():
    s = HTMLSession()
    for page in range(1, 21):
        url = f'https://www.monetnik.ru/monety/page.{page}/?sort=buyoutPrice_DESC'
        response = s.get(url=url, headers=headers)  # , proxies=proxies)
        cards = response.html.xpath('//div[@class="found"]/div')
        for card in cards:
            link = card.xpath('.//a/@href')[-1]

            yield link


async def get_items(s, link):
    items_res = []
    page_html = await s.get(link, headers=headers)  # , proxies=proxies)
    item_info = page_html.html.xpath(
        '//ul[@class="product-hero__infolist"]/li')
    info_table = []
    for row in item_info:
        try:
            key = row.xpath('.//span')[0].text.replace(':', '')
            value = row.xpath('.//span')[1].text
        except:
            key, value = None, None
        info_table.append((key, value))
    info_table = dict(info_table)
    title = page_html.html.xpath('//h1')[0].text
    price = page_html.html.xpath('//div[@class="product-hero__price"]')[0].text
    price = price.replace(' ', '').replace('"', '').split('₽')[0]
    image = page_html.html.xpath(
        '//div[@class="preview__modal--gal"]/descendant::img/@src')[0]

    page_details = {
        "Название": title,
        "Цена (руб.)": price,
        "Изображение": image,
        "Ссылка": link,
        "Т": info_table
    }
    items_res.append(page_details)

    return items_res


def save_to_db(data):
    df = pd.json_normalize(data, sep='-')
    conn = sqlite3.connect('async_monetnic.db')
    df.to_sql("monetnic_info", conn, index=False, if_exists='append')
    conn.close()
    # df.to_csv('monetnic.csv', index=False)


async def main(links):
    s = AsyncHTMLSession()
    tasks = (get_items(s, link) for link in links)
    return await asyncio.gather(*tasks)

if __name__ == '__main__':
    ua = UserAgent()
    headers = {'User-Agent': ua.random}
    # proxies = {'https': 'http://'}
    links = get_pages()
    result = asyncio.run(main(links))
    cleaned_data = [item[0] for item in result]
    save_to_db(cleaned_data)
