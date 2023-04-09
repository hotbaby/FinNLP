# encoding: utf8

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConflictError


ES_HOSTS = ["http://elastic:tvBeLtKMe5ZBwN1gQfIE@119.3.206.62:9200"]
ES_NEWS_INDEX = "innovest_research_news"
ES_DOC_ID = "{source}_{uuid}"

es_cli = Elasticsearch(ES_HOSTS)


def save_to_es(index, id, doc):
    try:
        es_cli.create(index, id, doc)
        return True

    except ConflictError:
        return True

    except Exception as e:
        # 当写Elasticsearch错误时，打印错误原因和原始文档。
        print(f"save_to_es error: {e}, index: {index}, doc: {doc}")
        return False
