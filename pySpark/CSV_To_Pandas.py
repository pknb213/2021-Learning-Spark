import json
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







