import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получает список из не более 1000 товаров магазина Ozon.
    
    Создает POST-запрос к Ozon Seller API методом `/v2/product/list` и возвращает содержимое ответа.
    
    Note:
        Метод запроса https://api-seller.ozon.ru/v2/product/list устарел и будет отключен с 9 февраля 2025 года.
        Документация к новой версии метода по [ссылке](https://docs.ozon.ru/api/seller/#operation/ProductAPI_GetProductListv3)

    Args:
        last_id (str): Идентификатор последнего значения на странице с товарами. 
        Используется для пагинации запросов.
        client_id (str): Идентификатор клиента (владельца магазина Ozon).
        seller_token (str): API-ключ владельца магазина Ozon.

    Returns:
        dict = {
            "items" : list[dict] = {
                    "offer_id": str - Идентификатор товара в системе продавца (артикул),
                    "product_id": int - Идентификатор товара в системе Ozon,
                } - Массив объектов, содержащих информацию о товарах,
            "last_id": str - Идентификатор последнего значения на странице с товарами,
            "total": int - Всего товаров на странице магазина,
        }: Содержимое ответа Ozon Seller API, dict["items"] включает не более 1000 объектов.
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получает список артикулов всех товаров магазина Ozon.
    
    Проводит пагинацию данных магазина OZON с помощью функции `get_product_list()`.

    Args:
        client_id (str): Идентификатор клиента (владельца магазина Ozon).
        seller_token (str): API-ключ владельца магазина Ozon.

    Returns:
        list[str]: Список артикулов всех товаров магазина Ozon.
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновляет цены товаров в магазине Ozon.
    
    Создает POST-запрос к Ozon Seller API методом `/v1/product/import/prices` и возвращает содержимое ответа.

    Args:
        prices (list): Массив из не более 1000 объектов, содержащих информацию о стоимости товаров.
        client_id (str): Идентификатор клиента (владельца магазина Ozon).
        seller_token (str): API-ключ владельца магазина Ozon.

    Returns:
        list[
            dict = {
            "result" : dict = {
                    "errors": list[dict] - Массив ошибок, возникших при обработке запроса,
                    "offer_id": str - Идентификатор товара в системе продавца (артикул),
                    "product_id": int - Идентификатор товара в системе Ozon,
                    "updated": bool - Успех обновления информации товара,
                },
            }, 
        ]: Массив объектов из ответа Ozon Seller API
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновляет информацию об остатках товаров в магазине Ozon.
    
    Создает POST-запрос к Ozon Seller API методом `/v1/product/import/stocks` и возвращает содержимое ответа.
    
    Note:
        Метод запроса https://api-seller.ozon.ru/v1/product/import/stocks в будущем будет отключен.
        Рекомендовано переключиться на [следующий метод](https://docs.ozon.ru/api/seller/#operation/ProductAPI_ProductsStocksV2)

    Args:
        stocks (list): Массив объектов, содержащих информацию об остатках товара.
        client_id (str): Идентификатор клиента (владельца магазина Ozon).
        seller_token (str): API-ключ владельца магазина Ozon.

    Returns:
        list[
            dict = {
            "result" : dict = {
                    "errors": list[dict] - Массив ошибок, возникших при обработке запроса,
                    "offer_id": str - Идентификатор товара в системе продавца (артикул),
                    "product_id": int - Идентификатор товара в системе Ozon,
                    "updated": bool - Успех обновления информации товара,
                },
            },
        ]: Массив объектов из ответа Ozon Seller API
    """
    
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Парсит с сайта casio данные об остатках часов на складе.
    
    Скачивает .zip файл по приведенной ссылке. 
    Извлекает из него в текущую папку .xls файл, читает его и преобразует содержимое в список словарей.
    Удаляет .xls файл после использования.
    
    Returns:
        list[dict]: Список словарей, где каждый словарь представляет одну строку данных об остатках часов на складе. 
        Ключи в каждом словаре соответствуют столбцам в скачиваемом .xls файле.
        
    """
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls") 
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Создает массив объектов, содержащих информацию об остатках часов, на основе данных со склада.
    
    Артикулы, отсутствующие в массиве данных со склада, включаются в итоговый массив с нулевым значением остатков.
        
    Args:
        watch_remnants (list[dict]): Список словарей, содержащих информацию об остатках часов на складе (массив данных со склада). 
        Возвращается функцией `download_stock()`.
        offer_ids (list[str]): Список артикулов всех товаров магазина Ozon.
        Возвращается функцией `get_offer_ids()`.

    Returns:
        list[
            dict = {
                "offer_id": str - Артикул товара в магазине Ozon,
                "stock": int - Значение остатков,
            },
        ]: Массив объектов, содержащих информацию об остатках товара.
      
    """
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создает массив объектов, содержащих информацию о ценах часов, на основе данных со склада.
    
    Значение цены из массива данных со склада форматируется с помощью функции `price_conversion()`.

    Args:
        watch_remnants (list[dict]): Список словарей, содержащих информацию об остатках часов на складе (массив данных со склада). 
        Возвращается функцией `download_stock()`.
        offer_ids (list[str]): Список артикулов всех товаров магазина Ozon.
        Возвращается функцией `get_offer_ids()`.

    Returns:
        list[
            dict = {
                "auto_action_enabled": str - Атрибут Ozon Seller API для включения и выключения автоприменения акций,
                "currency_code": str - Код валюты,
                "offer_id": str - Артикул товара,
                "old_price": str - Цена до скидок в рублях,
                "price": str - Цена товара с учётом скидок,
            },
        ]: Массив объектов, каждый из которых содержит информацию о стоимости товара.
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Форматирует цену. Пример: 5'990.00 руб. -> 5990
    
    Args:
        price (str): Цена товара из массива данных со склада

    Returns:
        str: Цена товара в рублях без специальных символов
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Создает генератор, отделяющий от списка `lst` части по `n` элементов максимум в каждой.

    Args:
        lst (list): Список, разбиваемый на части
        n (int): Максимальное число элементов в 1 части

    Yields:
        list: Срез исходного списка, включающий `n` элементов
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Асинхронно обновляет цены в магазине Ozon.
    
    Note:
        Не запускается в рамках текущего скрипта.
    
    Args:
        watch_remnants (list[dict]): Список словарей, содержащих информацию об остатках часов на складе (массив данных со склада). 
        Возвращается функцией `download_stock()`.
        client_id (str): Идентификатор клиента (владельца магазина Ozon).
        seller_token (str): API-ключ владельца магазина Ozon.

    Returns:
        list[
            dict = {
                "auto_action_enabled": str - Атрибут Ozon Seller API для включения и выключения автоприменения акций,
                "currency_code": str - Код валюты,
                "offer_id": str - Артикул товара,
                "old_price": str - Цена до скидок в рублях,
                "price": str - Цена товара с учётом скидок,
            },
        ]: Массив объектов, каждый из которых содержит информацию о стоимости товара.
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Асинхронно обновляет данные об остатках товаров в магазине Ozon.
    
    Note:
        Не запускается в рамках текущего скрипта

    Args:
        watch_remnants (list[dict]): Список словарей, содержащих информацию об остатках часов на складе (массив данных со склада). 
        Возвращается функцией `download_stock()`.
        client_id (str): Идентификатор клиента (владельца магазина Ozon).
        seller_token (str): API-ключ владельца магазина Ozon.

    Returns:
        list[
            dict = {
                "offer_id": str - Артикул товара в магазине Ozon,
                "stock": int - Значение остатков,
            },
        ]: Фильтрованный массив объектов, содержащих ненулевую информацию об остатках товара
        
        list[
            dict = {
                "offer_id": str - Артикул товара в магазине Ozon,
                "stock": int - Значение остатков,
            },
        ]: Массив объектов, содержащих информацию об остатках товара
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    """Выполняет основную логику приложения.
    Обновляет данные об остатках товаров и их стоимости в магазине Ozon.
    """    
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
