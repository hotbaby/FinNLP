# encoding: utf8

import hashlib
from utils import save_to_es, ES_NEWS_INDEX
from finnlp.data_sources.news.sina_finance_date_range import Sina_Finance_Date_Range

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

start_date = "2023-03-01"
end_date = "2023-03-01"
config = {
    # "use_proxy": "china_free",   # use proxies to prvent ip blocking
    "max_retry": 5,
    "proxy_pages": 5,
}

news_downloader = Sina_Finance_Date_Range(config)
news_downloader.download_date_range_all(start_date, end_date)
news_downloader.gather_content()
df = news_downloader.dataframe
columns = ["oid", "url", "wapurl", "title", "intro", "content", "summary", "ctime"]


for row in df[columns].values:
    data = dict(zip(columns, row))

    if not data["url"]:
        print("url is empyty and continue...")
        continue

    try:
        data["ctime"].to_pydatetime().strftime(DATETIME_FORMAT)
    except Exception as e:
        print(f"convert datetime error {e} and contiue...")
        continue

    # 保存到Elasticsearch
    url = data["url"]
    url_hash = hashlib.md5(url.encode()).hexdigest()
    es_id = f"sina_{url_hash}"
    save_to_es(ES_NEWS_INDEX, es_id, data)
