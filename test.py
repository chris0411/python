# -*- coding: utf-8 -*-
"""
Created on Sat Mar 16 14:40:54 2019

@author: chris
"""

import pandas as pd
import numpy as np


exam_data = {'name': ['Anastasia', 'Dima', 'Katherine', 'James', 'Emily',
                      'Michael', 'Matthew', 'Laura', 'Kevin', 'Jonas'],
             'score': [12.5, 9, 16.5, np.nan, 9, 20, 14.5, np.nan, 8, 19],
             'attempts': [1, 3, 2, 3, 2, 3, 1, 1, 2, 1],
             'qualify': ['yes', 'no', 'yes', 'no', 'no', 'yes', 'yes', 'no',
                         'no', 'yes']}
labels = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']

df1 = pd.DataFrame(exam_data, index=labels)

df2 = df1.copy()

df1 = pd.DataFrame(np.random.randint(1, 100, (15, 3)), columns=list('abc'))


#df1.rolling.apply(rank, raw=None, args=2)


d = {'col1': [1, 2, 3, 4, 7, 11], 'col2': [4, 5, 6, 9, 5, 0], 'col3': [7, 5, 8, 12, 1,11]}
df = pd.DataFrame(data=d)
print("Original DataFrame")
print(df)
print("\ntopmost n records within each group of a DataFrame:")
df1 = df.nlargest(3, 'col1')
print(df1)
df2 = df.nlargest(3, 'col2')
print(df2)
df3 = df.nlargest(3, 'col3')
print(df3)



class myGlobal(object):
    a = 1
    

test = myGlobal().a
print('a=', test)

