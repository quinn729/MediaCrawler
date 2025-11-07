from typing import List, Dict

import config
from var import source_keyword_var

from ._store_impl import *


class HotTopicsStoreFactory:
    STORES = {
        "csv": HotTopicsCsvStoreImplement,
        "db": HotTopicsDbStoreImplement,
        "json": HotTopicsJsonStoreImplement,
        "sqlite": HotTopicsSqliteStoreImplement,
    }

    @staticmethod
    def create_store() -> AbstractStore:
        store_class = HotTopicsStoreFactory.STORES.get(config.SAVE_DATA_OPTION)
        if not store_class:
            raise ValueError(
                "[HotTopicsStoreFactory.create_store] Invalid save option only supported csv or db or json or sqlite ..."
            )
        return store_class()


async def save_news(news_item: Dict):
    """
    保存新闻到数据库
    Args:
        news_item: 新闻项字典
    """
    store = HotTopicsStoreFactory.create_store()
    await store.store_content(news_item)


async def save_topic(topic_item: Dict):
    """
    保存话题项

    Args:
        topic_item: 话题项字典
    """
    store = HotTopicsStoreFactory.create_store()
    await store.store_topic(topic_item)
