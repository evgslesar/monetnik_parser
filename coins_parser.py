import pandas as pd
import sqlite3
from requests_html import HTMLSession
from fake_useragent import FakeUserAgent


session = HTMLSession()
ua = FakeUserAgent()
headers = {'User-Agent': ua.random}
# proxies = {'https': ''}

def get_pages():
    for page in range(11, 21):
        url = f'https://www.monetnik.ru/monety/page.{page}/?sort=buyoutPrice_DESC'
        response = session.get(url=url, headers=headers) #, proxies=proxies)
        cards = response.html.xpath('//div[@class="found"]/div')
        for card in cards:
            link = card.xpath('.//a/@href')[-1]
            print(link)
            yield link

 
def get_items():
    items_res = []
    for link in get_pages():
        page_html = session.get(link, headers=headers) #, proxies=proxies)
        item_info = page_html.html.xpath('//ul[@class="product-hero__infolist"]/li')
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
        image = page_html.html.xpath('//div[@class="preview__modal--gal"]/descendant::img/@src')[0]
            
        page_details = {
            "Название": title,
            "Цена (руб.)": price,
            "Изображение": image,
            "Ссылка": link,
            "Т": info_table
        }
        items_res.append(page_details)

    return items_res

def save_to_db(items_res):
    df = pd.json_normalize(items_res, sep='-')
    conn = sqlite3.connect('monetnic.db')
    df.to_sql("monetnic_info", conn, index=False, if_exists='append')
    conn.close()
    # df.to_csv('monetnic.csv', index=False)


if __name__ == '__main__':
    items_res = get_items()
    save_to_db(items_res)
