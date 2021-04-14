import json
<<<<<<< HEAD
import sys
from timeit import default_timer as timer
from datetime import timedelta

# Basics
import numpy
import pandas as pd

# misc
import gc
import warnings

# viz
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import seaborn as sns

st = timer()
# data_file = open("yelp_academic_dataset_business.json", encoding='utf-8')
# data = []
# for line in data_file:
#     data.append(json.loads(line))
# business_df = pd.DataFrame(data)
# data_file.close()

# with open("C:\\Users\\pknb2\\Desktop\\DataSet\\yelp_academic_dataset_business.json", encoding='utf-8') as data:
#     business_df = pd.read_json(data, lines=True)
pd.options.display.max_columns = None
business_df = pd.read_json("C:\\Users\\pknb2\\Desktop\\DataSet\\yelp_academic_dataset_business.json", lines=True)
print(timedelta(seconds=timer()-st))
print(business_df.head())

x = business_df['stars'].value_counts()
x = x.sort_index()

plt.figure(figsize=(8, 4))
ax = sns.barplot(x.index, x.values, alpha=0.8)
plt.title("Star Rating Distribution")
plt.ylabel("# of businesses", fontsize=12)
plt.xlabel("Star Ratings", fontsize=12)

rects = ax.patches
labels = x.values
for rect, label in zip(rects, labels):
    height = rect.get_height()
    ax.text(rect.get_x() + rect.get_width()/2, height + 5, label, ha='center', va='bottom')

plt.show()





=======
import asyncio
import pandas as pd
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk
>>>>>>> NoteBook

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
