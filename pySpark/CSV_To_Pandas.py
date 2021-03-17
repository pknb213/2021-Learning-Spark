import json
import pandas as pd
data_file = open("yelp_academic_dataset_business.json", encoding='utf-8')
data = []
for line in data_file:
    data.append(json.loads(line))
business_df = pd.DataFrame(data)
data_file.close()
print(business_df)


