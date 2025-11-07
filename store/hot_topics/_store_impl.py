# 声明：本代码仅供学习和研究目的使用。使用者应遵守以下原则：
# 1. 不得用于任何商业用途。
# 2. 使用时应遵守目标平台的使用条款和robots.txt规则。
# 3. 不得进行大规模爬取或对平台造成运营干扰。
# 4. 应合理控制请求频率，避免给目标平台带来不必要的负担。
# 5. 不得用于任何非法或不当的用途。
#
# 详细许可条款请参阅项目根目录下的LICENSE文件。
# 使用本代码即表示您同意遵守上述原则和LICENSE中的所有条款。


# -*- coding: utf-8 -*-
# @Author  : persist1@126.com
# @Time    : 2025/9/5 19:34
# @Desc    : 热点新闻存储实现类
import asyncio
import csv
import json
import os
import pathlib
from typing import Dict

import aiofiles
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

import config
from base.base_crawler import AbstractStore
from database.db_session import get_session
from database.models import (
    DailyNews,
    DailyTopics,
)
from tools.async_file_writer import AsyncFileWriter
from tools import utils, words
from var import crawler_type_var


class HotTopicsCsvStoreImplement(AbstractStore):
    def __init__(self):
        self.file_writer = AsyncFileWriter(
            crawler_type=crawler_type_var.get(), platform="hot_topics"
        )

    async def store_content(self, content_item: Dict):
        """
        content CSV storage implementation
        Args:
            content_item:

        Returns:

        """
        await self.file_writer.write_to_csv(item=content_item, item_type="videos")

    async def store_comment(self, comment_item: Dict):
        """
        comment CSV storage implementation
        Args:
            comment_item:

        Returns:

        """
        await self.file_writer.write_to_csv(item=comment_item, item_type="comments")

    async def store_creator(self, creator: Dict):
        """
        creator CSV storage implementation
        Args:
            creator:

        Returns:

        """
        raise ValueError("creator CSV storage not implemented")

    async def store_contact(self, contact_item: Dict):
        """
        creator contact CSV storage implementation
        Args:
            contact_item: creator's contact item dict

        Returns:

        """
        await self.file_writer.write_to_csv(item=contact_item, item_type="contacts")

    async def store_dynamic(self, dynamic_item: Dict):
        """
        creator dynamic CSV storage implementation
        Args:
            dynamic_item: creator's contact item dict

        Returns:

        """
        await self.file_writer.write_to_csv(item=dynamic_item, item_type="dynamics")


class HotTopicsDbStoreImplement(AbstractStore):
    """
    HotTopics DB storage implementation
    """

    async def store_content(self, content_item: Dict):
        """
        HotTopics content DB storage implementation
        for daily news store
        Args:
            content_item: content item dict
        """
        news_id = content_item.get("news_id")
        source_platform = content_item.get("source_platform")
        crawl_date = content_item.get("crawl_date")
        
        async with get_session() as session:
            result = await session.execute(
                select(DailyNews).where(
                    DailyNews.news_id == news_id,
                    DailyNews.source_platform == source_platform,
                    DailyNews.crawl_date == crawl_date
                )
            )
            news_detail = result.scalar_one_or_none()

            if not news_detail:
                content_item["add_ts"] = utils.get_current_timestamp()
                content_item["last_modify_ts"] = utils.get_current_timestamp()
                new_content = DailyNews(**content_item)
                session.add(new_content)
            else:
                for key, value in content_item.items():
                    setattr(news_detail, key, value)
                news_detail.last_modify_ts = utils.get_current_timestamp()
            await session.commit()

    async def store_comment(self, comment_item: Dict):
        """
        HotTopics comment DB storage implementation
        Args:
            comment_item: comment item dict
        """
        # 热点新闻模块暂不支持评论存储
        pass

    async def store_creator(self, creator: Dict):
        """
        HotTopics creator DB storage implementation
        Args:
            creator: creator item dict
        """
        # 热点新闻模块暂不支持创作者存储
        pass

    async def store_contact(self, contact_item: Dict):
        """
        HotTopics contact DB storage implementation
        Args:
            contact_item: contact item dict
        """
        # 热点新闻模块暂不支持联系方式存储
        pass

    async def store_dynamic(self, dynamic_item):
        """
        HotTopics dynamic DB storage implementation
        Args:
            dynamic_item: dynamic item dict
        """
        # 热点新闻模块暂不支持动态存储
        pass
    
    async def store_topic(self, topic_item: Dict):
        """
        HotTopics topic DB storage implementation
        Args:
            topic_item: topic item dict
        """
        topic_id = topic_item.get("topic_id")
        extract_date = topic_item.get("extract_date")
        
        async with get_session() as session:
            result = await session.execute(
                select(DailyTopics).where(
                    DailyTopics.topic_id == topic_id,
                    DailyTopics.extract_date == extract_date
                )
            )
            topic_detail = result.scalar_one_or_none()

            if not topic_detail:
                topic_item["add_ts"] = utils.get_current_timestamp()
                topic_item["last_modify_ts"] = utils.get_current_timestamp()
                new_topic = DailyTopics(**topic_item)
                session.add(new_topic)
            else:
                for key, value in topic_item.items():
                    setattr(topic_detail, key, value)
                topic_detail.last_modify_ts = utils.get_current_timestamp()
            await session.commit()


class HotTopicsJsonStoreImplement(AbstractStore):
    def __init__(self):
        self.file_writer = AsyncFileWriter(
            crawler_type=crawler_type_var.get(), platform="hot_topics"
        )

    async def store_content(self, content_item: Dict):
        """
        content JSON storage implementation
        Args:
            content_item:

        Returns:

        """
        await self.file_writer.write_single_item_to_json(
            item=content_item, item_type="contents"
        )

    async def store_comment(self, comment_item: Dict):
        """
        comment JSON storage implementation
        Args:
            comment_item:

        Returns:

        """
        await self.file_writer.write_single_item_to_json(
            item=comment_item, item_type="comments"
        )

    async def store_creator(self, creator: Dict):
        """
        creator JSON storage implementation
        Args:
            creator:

        Returns:

        """
        await self.file_writer.write_single_item_to_json(
            item=creator, item_type="creators"
        )

    async def store_contact(self, contact_item: Dict):
        """
        creator contact JSON storage implementation
        Args:
            contact_item: creator's contact item dict

        Returns:

        """
        await self.file_writer.write_single_item_to_json(
            item=contact_item, item_type="contacts"
        )

    async def store_dynamic(self, dynamic_item: Dict):
        """
        creator dynamic JSON storage implementation
        Args:
            dynamic_item: creator's contact item dict

        Returns:

        """
        await self.file_writer.write_single_item_to_json(
            item=dynamic_item, item_type="dynamics"
        )

    async def store_topic(self, topic_item: Dict):
        """
        topic JSON storage implementation
        Args:
            topic_item: topic item dict

        Returns:

        """
        await self.file_writer.write_single_item_to_json(
            item=topic_item, item_type="topics"
        )


class HotTopicsSqliteStoreImplement(HotTopicsDbStoreImplement):
    pass
