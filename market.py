import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получает список из не более 200 товаров магазина на Яндекс Маркет.
    
    Создает GET-запрос к странице магазина через API Яндекс Маркета и возвращает содержимое ответа.
    
    Note:
        Устаревший метод. 
        К использованию рекомендован [следующий метод](https://yandex.ru/dev/market/partner-api/doc/ru/reference/business-assortment/getOfferMappings)

    Args:
        page (str): Идентификатор страницы c результатами.
        campaign_id (int): Идентификатор кампании в API и магазина в кабинете. 
        access_token (str): Авторизационный токен API.

    Returns:
        dict = {
            "paging" : dict - Токены предыдущей и следующей страниц,
            "offerMappingEntries": list[dict] - Массив объектов, содержащих информацию о товарах. Включает не более 200 объектов,
        }: Содержимое ответа API Яндекс Маркета.
    """    
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Обновляет информацию об остатках товаров в магазине на Яндекс Маркет.

    Args:
        stocks (dict): Массив данных об остатках товаров.
        campaign_id (int): Идентификатор кампании в API и магазина в кабинете. 
        access_token (str): Авторизационный токен API
        
    Returns:
        dict: Содержит данные о статусе выполнения запроса.
    """    
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Обновляет цены товаров в магазине на Яндекс Маркет.

    Args:
        prices (list[dict]): Массив данных о стоимости товаров.
        campaign_id (int): Идентификатор кампании в API и магазина в кабинете. 
        access_token (str): Авторизационный токен API.

    Returns:
        dict: Содержит данные о статусе выполнения запроса и о возникших ошибках.
    """    
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получает список артикулов всех товаров магазина на Яндекс Маркет.

    Args:
        campaign_id (int): Идентификатор кампании в API и магазина в кабинете. 
        market_token (str): Авторизационный токен API.
        
    Returns:
        list[str]: Список артикулов всех товаров магазина на Яндекс Маркет.
    """    
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Создает массив объектов, содержащих информацию об остатках часов, на основе данных со склада.

    Args:
        watch_remnants (list[dict]): Список словарей, содержащих информацию об остатках часов на складе (массив данных со склада). 
        offer_ids (list[str]): Список артикулов всех товаров магазина на Яндекс Маркет.
        warehouse_id (str): Идентификатор склада Маркета
        
    Returns:
        list[dict]: Массив, включающий артикулы товаров, ID складов и информацию об остатках.
        
    """    
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создает массив объектов, содержащих информацию о ценах часов, на основе данных со склада.

    Args:
        watch_remnants (list[dict]): Список словарей, содержащих информацию об остатках часов на складе (массив данных со склада). 
        offer_ids (list[str]): Список артикулов всех товаров магазина на Яндекс Маркет.

    Returns:
        list[dict]: Массив, включающий артикулы товаров, и информацию об их стоимости.
    """    
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Асинхронно обновляет цены в магазине на Яндекс Маркет.
    
    Note:
        Не запускается в рамках текущего скрипта.

    Args:
        watch_remnants (list[dict]): Список словарей, содержащих информацию об остатках часов на складе (массив данных со склада). 
        campaign_id (int): Идентификатор кампании в API и магазина в кабинете. 
        market_token (str): Авторизационный токен API.

    Returns:
        list[dict]: Массив, включающий артикулы товаров, и информацию об их стоимости.
    """    
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Асинхронно обновляет данные об остатках товаров в магазине на Яндекс Маркет.

    Args:
        watch_remnants (list[dict]): Список словарей, содержащих информацию об остатках часов на складе (массив данных со склада). 
        campaign_id (int): Идентификатор кампании в API и магазина в кабинете. 
        market_token (str): Авторизационный токен API.
        warehouse_id (str): Идентификатор склада Маркета

    Returns:
        list[dict]: Фильтрованный массив, включающий информацию только о товарах с ненулевым остатком: артикулы, ID складов и информацию об остатках.
        list[dict]: Массив, включающий артикулы всех товаров, ID складов и информацию об остатках.
    """    
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    """Выполняет основную логику приложения.
    Обновляет данные об остатках товаров и их стоимости в магазине на Яндекс Маркет.
    Обновление данных происходит раздельно для моделей FBS (Fulfillment by Seller) и DBS (Delivery by Seller).
    """    
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
