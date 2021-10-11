# Data Cleansing
import pandas as pd
df = pd.DataFrame()
# 1.  Explore data: df.head(), df.tail(), df.info(), df.describe()
#  Check NULL values totals: df.isna().sum()
# 2.  Drop NaValues: df.dropna(inplace=True)
# 3.  Deal with Duplicates: df.duplicated().value_counts()
''' A general rule of thumb is to ignore the duplicate values if they are less 
than 0.5%. This is because, if the proportion is very low, duplicate values can 
also be because of chance. If it is higher than 0.5%, you can check if the 
consecutive values are duplicate. If the consecutive values are duplicate, you can 
drop them.'''
df.loc[(df['col_1'].diff() != 0) | 
       (df['col_2'].diff() != 0) | 
       (df['col_3'].diff() != 0) |
       (df['col_4'].diff() != 0)
       ]
# 4.  Row count: df.shape[0]
# 5.  Deal with outliers: df['returns'] = df['close'].pct_change()
