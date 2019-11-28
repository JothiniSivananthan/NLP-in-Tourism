from pyspark.sql import SparkSession
import pandas as pd
import pandas as pd
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql import SparkSession

from functools import reduce
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import pandas as pd
import re
import string

import pandas as pd
import re
import string
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import pyspark.sql.functions
from functools import reduce
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import pandas as pd
import re
import string
import pandas as pd
import re
import string
from collections import Counter

df = pd.read_csv("HotelInformation"
                 ".csv")


spark = SparkSession \
    .builder \
    .appName("Python Spark create RDD example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
df = pd.read_csv("HotelInformation"
                 ".csv")


df['AmenitiesList'] =  [re.sub(r'Room Service','RoomService', str(x)) for x in df['AmenitiesList']]

df['AmenitiesList'] =  [re.sub(r'Swimming Pool','SwimmingPool', str(x)) for x in df['AmenitiesList']]

df['AmenitiesList'] = [re.sub(r'Coffee Shop','CoffeeShop', str(x)) for x in df['AmenitiesList']]

df['AmenitiesList'] = df['AmenitiesList'].str.replace('+','')
spark_df = spark.createDataFrame(df)
spark_df.show()
num_cols =['DiscountedRoomPrice']
spark_df.select(num_cols).describe().show()
#a = df['DiscountedRoomPrice']
#desc1 = spark_df['DiscountedRoomPrice']
#tats = desc1.describe()
#print(stats)
#reviews_rdd = spark_df.select("AmenitiesList").rdd.flatMap(lambda x: x)
#reviews_rdd.collect()
a = df['AmenitiesList'].str.split()
b = df['AmenitiesList']
print(a)
from nltk import FreqDist

from gensim import corpora


gensim_dictionary = corpora.Dictionary(a)
print(gensim_dictionary)
import heapq

wordfreq = {}
for sentence in b:
    tokens = nltk.word_tokenize(sentence)
    for token in tokens:
        if token not in wordfreq.keys():
            wordfreq[token] = 1
        else:
            wordfreq[token] += 1
most_freq = heapq.nlargest(200, wordfreq, key=wordfreq.get)
print(most_freq)
from collections import OrderedDict
from operator import itemgetter

print(OrderedDict(sorted(wordfreq.items(), key = itemgetter(1), reverse = False)))

from nltk.probability import FreqDist
fdist = FreqDist(tokens)
print(fdist)


fdist.most_common(2)

# Frequency Distribution Plot
import matplotlib.pyplot as plt
fdist.plot(30,cumulative=False)
plt.show()
import numpy as np
from wordcloud import WordCloud
indexes = np.arange(len(tokens))
width = 0.7
plt.bar(indexes, tokens, width)
plt.xticks(indexes + width * 0.5, tokens)
plt.show()