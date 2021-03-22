import json
import asyncio
import pandas as pd
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk

# data_file = open("yelp_academic_dataset_business.json", encoding='utf-8')
# data = []
# for line in data_file:
#     data.append(json.loads(line))
# business_df = pd.DataFrame(data)
# data_file.close()
# print(business_df)


es = AsyncElasticsearch(
    cloud_id="i-o-optimized-deployment:d2VzdHVzMi5henVyZS5lbGFzdGljLWNsb3VkLmNvbTo5MjQzJGI3N2MwODBkZjRhMDQ2NTNhMjBmODMyNGRjODMzZDNkJDg5ZTk3ZjdjYTBlYTRkN2E5MjUzY2ZmOGM2ZTljMTI5",
    http_auth=("elastic", "n2mEaI6hh9nwhWLnUhwLXIhb"),
)


async def data():
    doc = open("yelp_academic_dataset_review.json", encoding='utf-8')
    for i in doc:
        yield {
            "_index": "kaggle_review",
            "_source": json.loads(i)
        }


async def main():
    await async_bulk(es, data())

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
print("End")
