# encoding: utf8

import os
import json
import logging
import hashlib
import datetime
import redis
import numpy as np
import pandas as pd
from json.encoder import JSONEncoder
from utils import save_to_es, ES_NEWS_INDEX
from finnlp.data_sources.news.sina_finance_date_range import Sina_Finance_Date_Range

from typing import Any, List, Dict

logger = logging.getLogger("app")


DATE_FROMAT = "%Y-%m-%d"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

REDIS_CRAWLER_TASK_KEY = "data:crawler"
REDIS_CRAWLER_RUNNING_TASK_KEY = "data:crawler:running:{source}_{date}"

REDIS_HOST = "119.3.206.62"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD")

redis_cli = redis.Redis(host=REDIS_HOST, port=REDIS_PORT,
                        db=REDIS_DB, password=REDIS_PASSWORD)


class CustomJSONEncoder(JSONEncoder):

    def default(self, o: Any) -> Any:
        if isinstance(o, datetime.datetime):
            return o.strftime(DATETIME_FORMAT)
        elif isinstance(o, datetime.date):
            return o.strftime(DATE_FROMAT)
        else:
            return super().default(o)


def download(start_date, end_date, max_retry: int = 5, proxy_pages: int = 5,
             download_dir: str = "~/Downloads/news/sina"):

    download_dir = os.path.expanduser(download_dir)
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    config = {
        # "use_proxy": "china_free",   # use proxies to prvent ip blocking
        "max_retry": max_retry,
        "proxy_pages": proxy_pages,
    }

    news_downloader = Sina_Finance_Date_Range(config)
    news_downloader.download_date_range_all(start_date, end_date)
    news_downloader.gather_content()
    df = news_downloader.dataframe
    columns = ["oid", "url", "wapurl", "title", "intro", "content", "summary", "ctime"]

    counter = 0
    for row in df[columns].values:
        data = dict(zip(columns, row))

        if not data["url"]:
            logger.info("url is empyty and continue...")
            continue

        try:
            data["ctime"] = data["ctime"].to_pydatetime()
        except Exception as e:
            logger.info(f"convert datetime error {e} and contiue...")
            continue

        if not data["content"] or data["content"] is np.nan:
            print("content is empty, continue...")
            continue

        # 保存到Elasticsearch
        url = data["url"]
        url_hash = hashlib.md5(url.encode()).hexdigest()
        es_id = f"sina_{url_hash}"
        save_to_es(ES_NEWS_INDEX, es_id, data)

        # 保存到本地
        date_str = data["ctime"].strftime(DATE_FROMAT)
        file_path = os.path.join(download_dir, date_str, f"{es_id}.json")
        if not os.path.exists(os.path.dirname(file_path)):
            os.makedirs(os.path.dirname(file_path))

        with open(file_path, "w+") as f:
            f.write(json.dumps(data, cls=CustomJSONEncoder, ensure_ascii=False))

        logger.info(f"persistant document {es_id}")
        counter += 1

    logger.info(f"from {start_date} to {end_date}, save {counter} docs")


def producer(start_date, end_date):
    for date in pd.date_range(start_date, end_date):
        date_str = date.to_pydatetime().strftime(DATE_FROMAT)
        task_data = {
            "date": date_str,
            "source": "sina"
        }
        redis_cli.sadd(REDIS_CRAWLER_TASK_KEY, json.dumps(task_data, ensure_ascii=False))


def consumer():
    while True:
        try:
            task_str = redis_cli.spop(REDIS_CRAWLER_TASK_KEY)
            if not task_str:
                logger.info("task queue is empty!")
                break

            task = json.loads(task_str)
            key = REDIS_CRAWLER_RUNNING_TASK_KEY.format(source=task["source"], date=task["date"])
            redis_cli.set(key, "true")
            date_str = task["date"]
            logger.info(f"download {date_str} data...")
            download(date_str, date_str)
            redis_cli.delete(key)
        except Exception as e:
            logger.error(e)
            logger.exception(e)


if __name__ == "__main__":
    # producer("2022-09-01", "2023-03-08")
    logging.basicConfig(format=logging.BASIC_FORMAT, level=logging.INFO)
    consumer()
