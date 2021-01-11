#!/usr/bin/env python
# coding: utf-8

# In[1]:


#Sentiment Analysis of movies reviews
# Databricks


# In[2]:


path = '/FileStore/tables/movies/cv*.txt'
reviewRdd =  sc.wholeTextFiles(path)
#reviewR.collect()

path = '/FileStore/tables/pos.txt'
posR = sc.textFile(path)
pos = posR.collect()
posR.collect()
#print(pos)

path = '/FileStore/tables/neg.txt'
negR = sc.textFile(path)
neg = negR.collect()
#print(neg)


# I am not interested in the filename, so I created an RDD with only the text
reviewRdd = reviewRdd.map(lambda x:x[1])
reviewRdd = reviewRdd.map(lambda x:x.replace('\n',''))
reviewRdd = reviewRdd.map(lambda x:x.split(' '))
reviewRdd = reviewRdd.map(lambda x: (len([1 for y in x if y in pos])-len([1 for y in x if y in neg])))
sentimentRdd = reviewRdd.map(lambda x: 'Positive' if x>=0 else 'Negative')
sentimentRdd.collect()
#print(sentimentRdd)


# In[3]:




