import asyncio
import json
import aiohttp
import asyncpg
from datetime import datetime
from typing import Dict, Any, Optional, List
from loguru import #logger
import os

# Constants
API_URL = 'https://api.store.4partners.io/partner/v1/rubric/product-list-full/1184363'
API_TOKEN = '7517b7af0ec749a87c72ba1e443eea0fa7f29e3e0945f22a8a78ec4355567eb7f3242bd46f9d99379c867578cddfc9f3fc3f0e6a4e62fbf2e9c27c81'
FETCH_INTERVAL = 180  # 3 minutes in seconds

# Настройка логирования
#logger.add("poizon_parser.log", rotation="500 MB", level="INFO")

class APIClient:
    def __init__(self
        self.session = None
        self.headers = {'X-Auth-Token': API_TOKEN}

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(headers=self.headers)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def get_products(self, scroll_id: Optional[str] = None) -> Dict[str, Any]:
        """Получение списка товаров с пагинацией"""
        params = {'vendorIds': '1339', 'limit': 100}
        if scroll_id:
            params['scroll'] = scroll_id

        try:
            async with self.session.get(API_URL, params=params, timeout=300) as response:
                if response.status != 200:
                    #logger.error(f"API error: {response.status} - {await response.text()}")
                    return {'products': [], 'next_scroll_id': None}

                data = await response.json()
                if data['status'] != 'ok':
                    #logger.error(f"API error: {data['message']}")
                    return {'products': [], 'next_scroll_id': None}

                return {
                    'products': data['result']['items'],
                    'next_scroll_id': data['result']['scroll']['id'] if data['result'].get('scroll') else None
                }
        except Exception as e:
            ##logger.error(f"API request error: {e}")
            return {'products': [], 'next_scroll_id': None}

    async def get_prices(self, product_ids: List[int]) -> Dict[str, Any]:
        """Получение цен вариаций товаров"""
        url = 'https://api.store.4partners.io/partner/v1/product/items/prices'
        params = {
            'country': 'ru',
            'currency': 'rub'
        }
        data = {
            'product_ids': product_ids
        }

        try:
            async with self.session.post(url, params=params, json=data, timeout=300) as response:
                if response.status != 200:
                    #logger.error(f"Price API error: {response.status} - {await response.text()}")
                    return {'items': []}

                data = await response.json()
                if data['status'] != 'ok':
                    #logger.error(f"Price API error: {data['message']}")
                    return {'items': []}

                return data['result']
        except Exception as e:
            #logger.error(f"Price API request error: {e}")
            return {'items': []}

class PoizonFetcher:
    def __init__(self):
        self.db_pool = None
        self.scroll_id = None
        self.last_processed_id = None
        self.is_running = True

    async def init_db(self):
        """Инициализация подключения к базе данных"""
        try:
            self.db_pool = await asyncpg.create_pool(
                os.getenv('DATABASE_URL'),
                min_size=1,
                max_size=10
            )
            #logger.info("Database connection established")
        except Exception as e:
            #logger.error(f"Failed to connect to database: {e}")
            raise

    async def load_parsing_state(self) -> Optional[Dict[str, Any]]:
        """Загрузка последнего состояния парсинга"""
        async with self.db_pool.acquire() as conn:
            state = await conn.fetchrow(
                """
                SELECT scroll_id, last_processed_id, status
                FROM parsing_state
                WHERE status = 'active'
                ORDER BY last_processed_at DESC
                LIMIT 1
                """
            )
            if state:
                self.scroll_id = state['scroll_id']
                self.last_processed_id = state['last_processed_id']
                #logger.info(f"Loaded parsing state: scroll_id={self.scroll_id}, last_id={self.last_processed_id}")
                return dict(state)
            return None

    async def save_parsing_state(self):
        """Сохранение текущего состояния парсинга"""
        async with self.db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO parsing_state (scroll_id, last_processed_id, status)
                VALUES ($1, $2, 'active')
                ON CONFLICT (id) DO UPDATE
                SET scroll_id = $1,
                    last_processed_id = $2,
                    last_processed_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                """,
                self.scroll_id,
                self.last_processed_id
            )
            #logger.debug(f"Saved parsing state: scroll_id={self.scroll_id}, last_id={self.last_processed_id}")

    async def save_product(self, product_data: Dict[str, Any]):
        """Сохранение товара в базу данных"""
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                # Сохраняем основной товар
                product_id = await conn.fetchval(
                    """
                    INSERT INTO products (id, name, primary_rubric_id, rubric_ids, brand, description, description_clear)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (id) DO UPDATE
                    SET name = $2,
                        primary_rubric_id = $3,
                        rubric_ids = $4,
                        brand = $5,
                        description = $6,
                        description_clear = $7,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING id
                    """,
                    product_data['id'],
                    product_data['name'],
                    product_data['primary_rubric_id'],
                    product_data['rubric_ids'],
                    product_data['brand'],
                    product_data['description'],
                    product_data['description_clear']
                )

                # Получаем актуальные цены вариаций
                async with APIClient() as api_client:
                    price_data = await api_client.get_prices([product_id])
                    price_map = {}
                    for item in price_data['items']:
                        if item['id'] == product_id:
                            for variation in item['variations']:
                                price_map[variation['id']] = {
                                    'price': variation['price'],
                                    'quantity': variation['quantity']
                                }

                # Сохраняем характеристики товара
                for aspect in product_data['aspects']:
                    group_name = aspect['aspect_group']
                    for param in product_data['params']:
                        if param['aspect_id'] == aspect['id']:
                            # Получаем значение параметра из param_values
                            param_value = None
                            if 'param_values' in product_data:
                                for pv in product_data['param_values']:
                                    if pv['param_id'] == param['id']:
                                        param_value = pv['value']
                                        break
                            
                            if param_value is None:
                                param_value = param.get('value', param.get('name', ''))
                            
                            await conn.execute(
                                """
                                INSERT INTO product_attributes (product_id, name, value, group_name)
                                VALUES ($1, $2, $3, $4)
                                ON CONFLICT (product_id, name) DO UPDATE
                                SET value = $3,
                                    group_name = $4,
                                    updated_at = CURRENT_TIMESTAMP
                                """,
                                product_id,
                                param['name'],
                                param_value,
                                group_name
                            )

                # Сохраняем вариации
                for variation in product_data['variations']:
                    variation_id = await conn.fetchval(
                        """
                        INSERT INTO product_variations (
                            id, sku, product_id, description, original_price,
                            price, quantity, currency, url, warehouse,
                            vendor_id
                        )
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                        ON CONFLICT (id) DO UPDATE
                        SET sku = $2,
                            description = $4,
                            original_price = $5,
                            price = $6,
                            quantity = $7,
                            currency = $8,
                            url = $9,
                            warehouse = $10,
                            vendor_id = $11,
                            updated_at = CURRENT_TIMESTAMP
                        RETURNING id
                        """,
                        variation['id'],
                        variation['sku'],
                        product_id,
                        variation['description'],
                        variation['original_price'],
                        price_map.get(variation['id'], {}).get('price', variation['price']),
                        price_map.get(variation['id'], {}).get('quantity', variation['quantity']),
                        variation['currency'],
                        variation['url'],
                        variation['warehouse'],
                        variation['vendor_id']
                    )

                    # Сохраняем характеристики вариации
                    for param_id in variation['param_ids']:
                        param = next((p for p in product_data['params'] if p['id'] == param_id), None)
                        if param:
                            aspect = next((a for a in product_data['aspects'] if a['id'] == param['aspect_id']), None)
                            if aspect:
                                # Получаем значение параметра из param_values
                                param_value = None
                                if 'param_values' in product_data:
                                    for pv in product_data['param_values']:
                                        if pv['param_id'] == param_id:
                                            param_value = pv['value']
                                            break
                                
                                if param_value is None:
                                    param_value = param.get('value', param.get('name', ''))
                                
                                await conn.execute(
                                    """
                                    INSERT INTO variation_attributes (variation_id, name, value, group_name)
                                    VALUES ($1, $2, $3, $4)
                                    ON CONFLICT (variation_id, name) DO UPDATE
                                    SET value = $3,
                                        group_name = $4,
                                        updated_at = CURRENT_TIMESTAMP
                                    """,
                                    variation_id,
                                    param['name'],
                                    param_value,
                                    aspect['aspect_group']
                                )

                    # Сохраняем изображения вариации
                    for image_url in variation['images']:
                        await conn.execute(
                            """
                            INSERT INTO variation_images (variation_id, image_url)
                            VALUES ($1, $2)
                            ON CONFLICT DO NOTHING
                            """,
                            variation_id,
                            image_url
                        )

    async def fetch_products(self):
        """Основной метод получения товаров"""
        try:
            # Загружаем последнее состояние
            await self.load_parsing_state()

            while self.is_running:
                try:
                    async with APIClient() as api_client:
                        response = await api_client.get_products(scroll_id=self.scroll_id)
                        products = response['products']
                        self.scroll_id = response['next_scroll_id']
                        
                        if not products:
                            #logger.info("No more products to process")
                            break
                        
                        for product in products:
                            await self.save_product(product)
                            self.last_processed_id = product['id']
                            await self.save_parsing_state()
                            #logger.info(f"Processed product {product['id']}")

                except Exception as e:
                    #logger.error(f"Error during product fetching: {e}")
                    await asyncio.sleep(5)  # Задержка перед повторной попыткой

        except Exception as e:
            #logger.error(f"Critical error in fetch_products: {e}")
        finally:
            if self.db_pool:
                await self.db_pool.close()

    async def run(self):
        """Запуск фетчера"""
        try:
            await self.init_db()
            await self.fetch_products()
        except Exception as e:
            #logger.error(f"Error in fetcher run: {e}")
        finally:
            self.is_running = False

def main():
    """Точка входа"""
    fetcher = PoizonFetcher()
    asyncio.run(fetcher.run())

if __name__ == "__main__":
    main()
