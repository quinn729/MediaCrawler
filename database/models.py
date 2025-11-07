from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    Text,
    String,
    BigInteger,
    Date,
    Float,
    UniqueConstraint,
    Index,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


class DailyNews(Base):
    __tablename__ = "daily_news"
    __comment__ = "每日新闻表"

    id = Column(Integer, primary_key=True)
    news_id = Column(String(255), nullable=False, comment="新闻唯一ID")
    source_platform = Column(
        String(32),
        nullable=False,
        index=True,
        comment="新闻源平台(weibo|zhihu|bilibili|toutiao|douyin等)",
    )
    title = Column(String(512), nullable=False, comment="新闻标题")
    url = Column(String(512), comment="新闻链接")
    description = Column(Text, comment="新闻描述或摘要")
    extra_info = Column(Text, comment="额外信息(JSON格式存储)")
    crawl_date = Column(String(10), index=True, nullable=False, comment="爬取日期")
    rank_position = Column(Integer, index=True, comment="在热榜中的排名位置")
    add_ts = Column(BigInteger, nullable=False, comment="记录添加时间戳")
    last_modify_ts = Column(BigInteger, nullable=False, comment="记录最后修改时间戳")
    __table_args__ = (
        UniqueConstraint(
            "news_id",
            "source_platform",
            "crawl_date",
            name="idx_news_id_source_platform_crawl_date",
        ),
    )


class DailyTopics(Base):
    __tablename__ = "daily_topics"
    __comment__ = "每日提取话题表"

    id = Column(Integer, primary_key=True, comment="自增ID")
    topic_id = Column(String(64), nullable=False, comment="话题唯一ID")
    topic_name = Column(String(255), nullable=False, comment="话题名称")
    topic_description = Column(Text, comment="话题描述")
    keywords = Column(Text, comment="话题关键词(JSON格式存储)")
    extract_date = Column(Date, nullable=False, index=True, comment="话题提取日期")
    relevance_score = Column(Float, index=True, comment="话题相关性得分")
    news_count = Column(Integer, default=0, comment="关联的新闻数量")
    processing_status = Column(
        String(16),
        default="pending",
        index=True,
        comment="处理状态(pending|processing|completed|failed)",
    )
    add_ts = Column(BigInteger, nullable=False, comment="记录添加时间戳")
    last_modify_ts = Column(BigInteger, nullable=False, comment="记录最后修改时间戳")
    __table_args__ = (
        UniqueConstraint("topic_id", "extract_date", name="idx_daily_topics_unique"),
    )


class CrawlingTasks(Base):
    __tablename__ = "crawling_tasks"
    id = Column(Integer, primary_key=True, comment="自增ID")
    task_id = Column(String(64), nullable=False, unique=True, comment="任务唯一ID")
    topic_id = Column(String(64), nullable=False, index=True, comment="关联的话题ID")
    platform = Column(
        String(32),
        nullable=False,
        index=True,
        comment="目标平台(xhs|dy|ks|bili|wb|tieba|zhihu)",
    )
    search_keywords = Column(Text, nullable=False, comment="搜索关键词(JSON格式存储)")
    task_status = Column(
        String(16),
        default="pending",
        index=True,
        comment="任务状态(pending|running|completed|failed|paused)",
    )
    start_time = Column(BigInteger, comment="任务开始时间戳")
    end_time = Column(BigInteger, comment="任务结束时间戳")
    total_crawled = Column(BigInteger, default=0, comment="已爬取内容数量")
    success_count = Column(BigInteger, default=0, comment="成功爬取数量")
    error_count = Column(BigInteger, default=0, comment="错误数量")
    error_message = Column(Text, comment="错误信息")
    config_params = Column(Text, comment="爬取配置参数(JSON格式)")
    scheduled_date = Column(Date, nullable=False, index=True, comment="话题提取日期")
    add_ts = Column(BigInteger, nullable=False, comment="记录添加时间戳")
    last_modify_ts = Column(BigInteger, nullable=False, comment="记录最后修改时间戳")


# ALTER TABLE `xhs_note`
# ADD COLUMN `topic_id` varchar(64) DEFAULT NULL COMMENT '关联的话题ID',
# ADD COLUMN `crawling_task_id` varchar(64) DEFAULT NULL COMMENT '关联的爬取任务ID';

# -- 为抖音视频表添加话题关联字段
# ALTER TABLE `douyin_aweme`
# ADD COLUMN `topic_id` varchar(64) DEFAULT NULL COMMENT '关联的话题ID',
# ADD COLUMN `crawling_task_id` varchar(64) DEFAULT NULL COMMENT '关联的爬取任务ID';

# -- 为快手视频表添加话题关联字段
# ALTER TABLE `kuaishou_video`
# ADD COLUMN `topic_id` varchar(64) DEFAULT NULL COMMENT '关联的话题ID',
# ADD COLUMN `crawling_task_id` varchar(64) DEFAULT NULL COMMENT '关联的爬取任务ID';

# -- 为B站视频表添加话题关联字段
# ALTER TABLE `bilibili_video`
# ADD COLUMN `topic_id` varchar(64) DEFAULT NULL COMMENT '关联的话题ID',
# ADD COLUMN `crawling_task_id` varchar(64) DEFAULT NULL COMMENT '关联的爬取任务ID';

# -- 为微博帖子表添加话题关联字段
# ALTER TABLE `weibo_note`
# ADD COLUMN `topic_id` varchar(64) DEFAULT NULL COMMENT '关联的话题ID',
# ADD COLUMN `crawling_task_id` varchar(64) DEFAULT NULL COMMENT '关联的爬取任务ID';

# -- 为贴吧帖子表添加话题关联字段
# ALTER TABLE `tieba_note`
# ADD COLUMN `topic_id` varchar(64) DEFAULT NULL COMMENT '关联的话题ID',
# ADD COLUMN `crawling_task_id` varchar(64) DEFAULT NULL COMMENT '关联的爬取任务ID';

# -- 为知乎内容表添加话题关联字段
# ALTER TABLE `zhihu_content`
# ADD COLUMN `topic_id` varchar(64) DEFAULT NULL COMMENT '关联的话题ID',
# ADD COLUMN `crawling_task_id` varchar(64) DEFAULT NULL COMMENT '关联的爬取任务ID';


class BilibiliVideo(Base):
    __tablename__ = "bilibili_video"
    id = Column(Integer, primary_key=True)
    video_id = Column(BigInteger, nullable=False, index=True, unique=True)
    video_url = Column(Text, nullable=False)
    user_id = Column(BigInteger, index=True)
    nickname = Column(Text)
    avatar = Column(Text)
    liked_count = Column(Integer)
    add_ts = Column(BigInteger)
    last_modify_ts = Column(BigInteger)
    video_type = Column(Text)
    title = Column(Text)
    desc = Column(Text)
    create_time = Column(BigInteger, index=True)
    disliked_count = Column(Text)
    video_play_count = Column(Text)
    video_favorite_count = Column(Text)
    video_share_count = Column(Text)
    video_coin_count = Column(Text)
    video_danmaku = Column(Text)
    video_comment = Column(Text)
    video_cover_url = Column(Text)
    source_keyword = Column(Text, default="")
    topic_id = Column(String(64), unique=True, comment="关联的话题ID")
    crawling_task_id = Column(String(64), unique=True, comment="关联的爬取任务ID")


class BilibiliVideoComment(Base):
    __tablename__ = "bilibili_video_comment"
    id = Column(Integer, primary_key=True)
    user_id = Column(String(255))
    nickname = Column(Text)
    sex = Column(Text)
    sign = Column(Text)
    avatar = Column(Text)
    add_ts = Column(BigInteger)
    last_modify_ts = Column(BigInteger)
    comment_id = Column(BigInteger, index=True)
    video_id = Column(BigInteger, index=True)
    content = Column(Text)
    create_time = Column(BigInteger)
    sub_comment_count = Column(Text)
    parent_comment_id = Column(String(255))
    like_count = Column(Text, default="0")


class BilibiliUpInfo(Base):
    __tablename__ = "bilibili_up_info"
    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, index=True)
    nickname = Column(Text)
    sex = Column(Text)
    sign = Column(Text)
    avatar = Column(Text)
    add_ts = Column(BigInteger)
    last_modify_ts = Column(BigInteger)
    total_fans = Column(Integer)
    total_liked = Column(Integer)
    user_rank = Column(Integer)
    is_official = Column(Integer)


class BilibiliContactInfo(Base):
    __tablename__ = "bilibili_contact_info"
    id = Column(Integer, primary_key=True)
    up_id = Column(BigInteger, index=True)
    fan_id = Column(BigInteger, index=True)
    up_name = Column(Text)
    fan_name = Column(Text)
    up_sign = Column(Text)
    fan_sign = Column(Text)
    up_avatar = Column(Text)
    fan_avatar = Column(Text)
    add_ts = Column(BigInteger)
    last_modify_ts = Column(BigInteger)


class BilibiliUpDynamic(Base):
    __tablename__ = "bilibili_up_dynamic"
    id = Column(Integer, primary_key=True)
    dynamic_id = Column(BigInteger, index=True)
    user_id = Column(String(255))
    user_name = Column(Text)
    text = Column(Text)
    type = Column(Text)
    pub_ts = Column(BigInteger)
    total_comments = Column(Integer)
    total_forwards = Column(Integer)
    total_liked = Column(Integer)
    add_ts = Column(BigInteger)
    last_modify_ts = Column(BigInteger)


class DouyinAweme(Base):
    __tablename__ = "douyin_aweme"
    id = Column(Integer, primary_key=True)
    user_id = Column(String(255))
    sec_uid = Column(String(255))
    short_user_id = Column(String(255))
    user_unique_id = Column(String(255))
    nickname = Column(Text)
    avatar = Column(Text)
    user_signature = Column(Text)
    ip_location = Column(Text)
    add_ts = Column(BigInteger)
    last_modify_ts = Column(BigInteger)
    aweme_id = Column(BigInteger, index=True)
    aweme_type = Column(Text)
    title = Column(Text)
    desc = Column(Text)
    create_time = Column(BigInteger, index=True)
    liked_count = Column(Text)
    comment_count = Column(Text)
    share_count = Column(Text)
    collected_count = Column(Text)
    aweme_url = Column(Text)
    cover_url = Column(Text)
    video_download_url = Column(Text)
    music_download_url = Column(Text)
    note_download_url = Column(Text)
    source_keyword = Column(Text, default="")
    topic_id = Column(String(64), unique=True, comment="关联的话题ID")
    crawling_task_id = Column(String(64), unique=True, comment="关联的爬取任务ID")


class DouyinAwemeComment(Base):
    __tablename__ = "douyin_aweme_comment"
    id = Column(Integer, primary_key=True)
    user_id = Column(String(255))
    sec_uid = Column(String(255))
    short_user_id = Column(String(255))
    user_unique_id = Column(String(255))
    nickname = Column(Text)
    avatar = Column(Text)
    user_signature = Column(Text)
    ip_location = Column(Text)
    add_ts = Column(BigInteger)
    last_modify_ts = Column(BigInteger)
    comment_id = Column(BigInteger, index=True)
    aweme_id = Column(BigInteger, index=True)
    content = Column(Text)
    create_time = Column(BigInteger)
    sub_comment_count = Column(Text)
    parent_comment_id = Column(String(255))
    like_count = Column(Text, default="0")
    pictures = Column(Text, default="")


class DyCreator(Base):
    __tablename__ = "dy_creator"
    id = Column(Integer, primary_key=True)
    user_id = Column(String(255))
    nickname = Column(Text)
    avatar = Column(Text)
    ip_location = Column(Text)
    add_ts = Column(BigInteger)
    last_modify_ts = Column(BigInteger)
    desc = Column(Text)
    gender = Column(Text)
    follows = Column(Text)
    fans = Column(Text)
    interaction = Column(Text)
    videos_count = Column(String(255))


class KuaishouVideo(Base):
    __tablename__ = "kuaishou_video"
    id = Column(Integer, primary_key=True)
    user_id = Column(String(64))
    nickname = Column(Text)
    avatar = Column(Text)
    add_ts = Column(BigInteger)
    last_modify_ts = Column(BigInteger)
    video_id = Column(String(255), index=True)
    video_type = Column(Text)
    title = Column(Text)
    desc = Column(Text)
    create_time = Column(BigInteger, index=True)
    liked_count = Column(Text)
    viewd_count = Column(Text)
    video_url = Column(Text)
    video_cover_url = Column(Text)
    video_play_url = Column(Text)
    source_keyword = Column(Text, default="")
    topic_id = Column(String(64), unique=True, comment="关联的话题ID")
    crawling_task_id = Column(String(64), unique=True, comment="关联的爬取任务ID")


class KuaishouVideoComment(Base):
    __tablename__ = "kuaishou_video_comment"
    id = Column(Integer, primary_key=True)
    user_id = Column(Text)
    nickname = Column(Text)
    avatar = Column(Text)
    add_ts = Column(BigInteger)
    last_modify_ts = Column(BigInteger)
    comment_id = Column(BigInteger, index=True)
    video_id = Column(String(255), index=True)
    content = Column(Text)
    create_time = Column(BigInteger)
    sub_comment_count = Column(Text)


class WeiboNote(Base):
    __tablename__ = "weibo_note"
    id = Column(Integer, primary_key=True)
    user_id = Column(String(255))
    nickname = Column(Text)
    avatar = Column(Text)
    gender = Column(Text)
    profile_url = Column(Text)
    ip_location = Column(Text, default="")
    add_ts = Column(BigInteger)
    last_modify_ts = Column(BigInteger)
    note_id = Column(BigInteger, index=True)
    content = Column(Text)
    create_time = Column(BigInteger, index=True)
    create_date_time = Column(String(255), index=True)
    liked_count = Column(Text)
    comments_count = Column(Text)
    shared_count = Column(Text)
    note_url = Column(Text)
    source_keyword = Column(Text, default="")
    topic_id = Column(String(64), unique=True, comment="关联的话题ID")
    crawling_task_id = Column(String(64), unique=True, comment="关联的爬取任务ID")


class WeiboNoteComment(Base):
    __tablename__ = "weibo_note_comment"
    id = Column(Integer, primary_key=True)
    user_id = Column(String(255))
    nickname = Column(Text)
    avatar = Column(Text)
    gender = Column(Text)
    profile_url = Column(Text)
    ip_location = Column(Text, default="")
    add_ts = Column(BigInteger)
    last_modify_ts = Column(BigInteger)
    comment_id = Column(BigInteger, index=True)
    note_id = Column(BigInteger, index=True)
    content = Column(Text)
    create_time = Column(BigInteger)
    create_date_time = Column(String(255), index=True)
    comment_like_count = Column(Text)
    sub_comment_count = Column(Text)
    parent_comment_id = Column(String(255))


class WeiboCreator(Base):
    __tablename__ = "weibo_creator"
    id = Column(Integer, primary_key=True)
    user_id = Column(String(255))
    nickname = Column(Text)
    avatar = Column(Text)
    ip_location = Column(Text)
    add_ts = Column(BigInteger)
    last_modify_ts = Column(BigInteger)
    desc = Column(Text)
    gender = Column(Text)
    follows = Column(Text)
    fans = Column(Text)
    tag_list = Column(Text)


class XhsCreator(Base):
    __tablename__ = "xhs_creator"
    id = Column(Integer, primary_key=True)
    user_id = Column(String(255))
    nickname = Column(Text)
    avatar = Column(Text)
    ip_location = Column(Text)
    add_ts = Column(BigInteger)
    last_modify_ts = Column(BigInteger)
    desc = Column(Text)
    gender = Column(Text)
    follows = Column(Text)
    fans = Column(Text)
    interaction = Column(Text)
    tag_list = Column(Text)


class XhsNote(Base):
    __tablename__ = "xhs_note"
    id = Column(Integer, primary_key=True)
    user_id = Column(String(255))
    nickname = Column(Text)
    avatar = Column(Text)
    ip_location = Column(Text)
    add_ts = Column(BigInteger)
    last_modify_ts = Column(BigInteger)
    note_id = Column(String(255), index=True)
    type = Column(Text)
    title = Column(Text)
    desc = Column(Text)
    video_url = Column(Text)
    time = Column(BigInteger, index=True)
    last_update_time = Column(BigInteger)
    liked_count = Column(Text)
    collected_count = Column(Text)
    comment_count = Column(Text)
    share_count = Column(Text)
    image_list = Column(Text)
    tag_list = Column(Text)
    note_url = Column(Text)
    source_keyword = Column(Text, default="")
    xsec_token = Column(Text)
    topic_id = Column(String(64), unique=True, comment="关联的话题ID")
    crawling_task_id = Column(String(64), unique=True, comment="关联的爬取任务ID")


class XhsNoteComment(Base):
    __tablename__ = "xhs_note_comment"
    id = Column(Integer, primary_key=True)
    user_id = Column(String(255))
    nickname = Column(Text)
    avatar = Column(Text)
    ip_location = Column(Text)
    add_ts = Column(BigInteger)
    last_modify_ts = Column(BigInteger)
    comment_id = Column(String(255), index=True)
    create_time = Column(BigInteger, index=True)
    note_id = Column(String(255))
    content = Column(Text)
    sub_comment_count = Column(Integer)
    pictures = Column(Text)
    parent_comment_id = Column(String(255))
    like_count = Column(Text)


class TiebaNote(Base):
    __tablename__ = "tieba_note"
    id = Column(Integer, primary_key=True)
    note_id = Column(String(644), index=True)
    title = Column(Text)
    desc = Column(Text)
    note_url = Column(Text)
    publish_time = Column(String(255), index=True)
    user_link = Column(Text, default="")
    user_nickname = Column(Text, default="")
    user_avatar = Column(Text, default="")
    tieba_id = Column(String(255), default="")
    tieba_name = Column(Text)
    tieba_link = Column(Text)
    total_replay_num = Column(Integer, default=0)
    total_replay_page = Column(Integer, default=0)
    ip_location = Column(Text, default="")
    add_ts = Column(BigInteger)
    last_modify_ts = Column(BigInteger)
    source_keyword = Column(Text, default="")
    topic_id = Column(String(64), unique=True, comment="关联的话题ID")
    crawling_task_id = Column(String(64), unique=True, comment="关联的爬取任务ID")


class TiebaComment(Base):
    __tablename__ = "tieba_comment"
    id = Column(Integer, primary_key=True)
    comment_id = Column(String(255), index=True)
    parent_comment_id = Column(String(255), default="")
    content = Column(Text)
    user_link = Column(Text, default="")
    user_nickname = Column(Text, default="")
    user_avatar = Column(Text, default="")
    tieba_id = Column(String(255), default="")
    tieba_name = Column(Text)
    tieba_link = Column(Text)
    publish_time = Column(String(255), index=True)
    ip_location = Column(Text, default="")
    sub_comment_count = Column(Integer, default=0)
    note_id = Column(String(255), index=True)
    note_url = Column(Text)
    add_ts = Column(BigInteger)
    last_modify_ts = Column(BigInteger)


class TiebaCreator(Base):
    __tablename__ = "tieba_creator"
    id = Column(Integer, primary_key=True)
    user_id = Column(String(64))
    user_name = Column(Text)
    nickname = Column(Text)
    avatar = Column(Text)
    ip_location = Column(Text)
    add_ts = Column(BigInteger)
    last_modify_ts = Column(BigInteger)
    gender = Column(Text)
    follows = Column(Text)
    fans = Column(Text)
    registration_duration = Column(Text)


class ZhihuContent(Base):
    __tablename__ = "zhihu_content"
    id = Column(Integer, primary_key=True)
    content_id = Column(String(64), index=True)
    content_type = Column(Text)
    content_text = Column(Text)
    content_url = Column(Text)
    question_id = Column(String(255))
    title = Column(Text)
    desc = Column(Text)
    created_time = Column(String(32), index=True)
    updated_time = Column(Text)
    voteup_count = Column(Integer, default=0)
    comment_count = Column(Integer, default=0)
    source_keyword = Column(Text)
    user_id = Column(String(255))
    user_link = Column(Text)
    user_nickname = Column(Text)
    user_avatar = Column(Text)
    user_url_token = Column(Text)
    add_ts = Column(BigInteger)
    last_modify_ts = Column(BigInteger)
    topic_id = Column(String(64), unique=True, comment="关联的话题ID")
    crawling_task_id = Column(String(64), unique=True, comment="关联的爬取任务ID")

    # persist-1<persist1@126.com>
    # 原因：修复 ORM 模型定义错误，确保与数据库表结构一致。
    # 副作用：无
    # 回滚策略：还原此行


class ZhihuComment(Base):
    __tablename__ = "zhihu_comment"
    id = Column(Integer, primary_key=True)
    comment_id = Column(String(64), index=True)
    parent_comment_id = Column(String(64))
    content = Column(Text)
    publish_time = Column(String(32), index=True)
    ip_location = Column(Text)
    sub_comment_count = Column(Integer, default=0)
    like_count = Column(Integer, default=0)
    dislike_count = Column(Integer, default=0)
    content_id = Column(String(64), index=True)
    content_type = Column(Text)
    user_id = Column(String(64))
    user_link = Column(Text)
    user_nickname = Column(Text)
    user_avatar = Column(Text)
    add_ts = Column(BigInteger)
    last_modify_ts = Column(BigInteger)


class ZhihuCreator(Base):
    __tablename__ = "zhihu_creator"
    id = Column(Integer, primary_key=True)
    user_id = Column(String(64), unique=True, index=True)
    user_link = Column(Text)
    user_nickname = Column(Text)
    user_avatar = Column(Text)
    url_token = Column(Text)
    gender = Column(Text)
    ip_location = Column(Text)
    follows = Column(Integer, default=0)
    fans = Column(Integer, default=0)
    anwser_count = Column(Integer, default=0)
    video_count = Column(Integer, default=0)
    question_count = Column(Integer, default=0)
    article_count = Column(Integer, default=0)
    column_count = Column(Integer, default=0)
    get_voteup_count = Column(Integer, default=0)
    add_ts = Column(BigInteger)
    last_modify_ts = Column(BigInteger)
