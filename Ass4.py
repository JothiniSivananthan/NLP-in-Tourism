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
df = spark.read.format('com.databricks.spark.csv')
df = pd.read_csv("HotelInformation"
                 ".csv")


#spark = SparkSession \
##    .builder \
  ##  .appName("Python Spark create RDD example") \
  #  .config("spark.some.config.option", "some-value") \
  #  .getOrCreate()
#df = spark.read.format('com.databricks.spark.csv')
#df = pd.read_csv("HotelInformation"
          #       ".csv")


df['AmenitiesList'] =  [re.sub(r'Room Service','RoomService', str(x)) for x in df['AmenitiesList']]

df['AmenitiesList'] =  [re.sub(r'Swimming Pool','SwimmingPool', str(x)) for x in df['AmenitiesList']]

df['AmenitiesList'] = [re.sub(r'Coffee Shop','CoffeeShop', str(x)) for x in df['AmenitiesList']]
df['AmenitiesList'] = [re.sub(r'More','', str(x)) for x in df['AmenitiesList']]
df['AmenitiesList'] = [re.sub(r'More','', str(x)) for x in df['AmenitiesList']]

df['AmenitiesList'] = df['AmenitiesList'].str.replace('\d+','')
df['AmenitiesList'] = df['AmenitiesList'].str.replace('+','')



df['AmenitiesList'] = [re.sub(r'More','', str(x)) for x in df['AmenitiesList']]

df.rename(columns={"Recommendations": "GuestRecommendations(%)"},inplace= True)
print(df)
df['GuestRecommendations(%)']  = [re.sub(r'Guests recommend this hotel','', str(x)) for x in df['GuestRecommendations(%)']]
df['GuestRecommendations(%)'] = df['GuestRecommendations(%)'].str.replace('%','')
df['GuestRecommendations(%)'] = pd.to_numeric(df['GuestRecommendations(%)'])
spark_df = spark.createDataFrame(df)
spark_df.show()
#a = df['DiscountedRoomPrice']
#desc1 = spark_df['DiscountedRoomPrice']
#tats = desc1.describe()
#print(stats)
reviews_rdd = spark_df.select("AmenitiesList").rdd.flatMap(lambda x: x)
reviews_rdd.collect()
def word_tokenize1(x):
    lowerW = x.lower()
    return nltk.word_tokenize(x)



def sent_TokenizeFunct(x):
    return nltk.sent_tokenize(x)

sentenceTokenizeRDD = reviews_rdd.map(sent_TokenizeFunct)

sentenceTokenizeRDD.collect()
print(sentenceTokenizeRDD.collect())


def word_TokenizeFunct(x):
    splitted = [word for line in x for word in line.split()]
    return splitted

wordTokenizeRDD = sentenceTokenizeRDD.map(word_TokenizeFunct)

print(wordTokenizeRDD.collect())


freqDistRDD = wordTokenizeRDD.flatMap(lambda x : nltk.FreqDist(x).most_common()).map(lambda x: x).reduceByKey(lambda x,y : x+y).sortBy(lambda x: x[1], ascending = False)
print(freqDistRDD)
df_fDist = freqDistRDD.toDF() #converting RDD to spark dataframe
df_fDist.createOrReplaceTempView("myTable")
df2 = spark.sql("SELECT _1 AS Keywords, _2 as Frequency from myTable limit 20") #renaming columns
pandD = df2.toPandas() #converting spark dataframes to pandas dataframes
print(pandD)

my_plot = pandD.plot(figsize = (10, 10),
              x = "Keywords", y = "Frequency", kind  = "bar", legend = False )
import numpy as np

my_plot
#bins = np.arange(0, 100, 5.0)
#plt.hist(pandD, bins, alpha=0.8, histtype='bar', color='gold',
#plt.hist(pandD, bins, alpha=0.8, histtype='bar', color='gold',
     #    ec='black',weights=np.zeros_like(pandD) + 100. / pandD.size)


plt.title("Most common services in Goa Hotels", fontsize = 28)
plt.xticks(size = 18)
plt.yticks(size = 18)
plt.ylabel("")
plt.show()


num_rdd = spark_df.select("DiscountedRoomPrice").rdd.flatMap(lambda x: x)
num_rdd.max(),num_rdd.min(), num_rdd.sum(),num_rdd.variance(),num_rdd.stdev()
print(num_rdd.max())
print(num_rdd.min())
print(num_rdd.mean())
print(num_rdd.stats())



num_rdd2 = spark_df.select("HotelName","GuestRecommendations(%)").rdd.map(lambda x:x)
print(num_rdd2)

df_fDists = num_rdd2.toDF() #converting RDD to spark dataframe
print(df_fDists)
df_fDists.show()
#df_fDists.createOrReplaceTempView("myTable")

pand = df_fDists.toPandas()
print(pand)


df_c = pand.sort_values('GuestRecommendations(%)')
my_plot2 = df_c.plot(figsize = (10, 10),
              x = "HotelName", y = "GuestRecommendations(%)", kind  = "bar", legend = False )
import numpy as np

my_plot2
#bins = np.arange(0, 100, 5.0)
#plt.hist(pandD, bins, alpha=0.8, histtype='bar', color='gold',
#plt.hist(pandD, bins, alpha=0.8, histtype='bar', color='gold',
     #    ec='black',weights=np.zeros_like(pandD) + 100. / pandD.size)



plt.title("Guest Recommendations for Goa Hotels", fontsize = 28)
plt.xticks(size = 18)
plt.yticks(size = 18)
plt.ylabel("Percentage",fontsize = 28)
plt.show()

num_rdd2 = spark_df.select("GuestRecommendations(%)").rdd.flatMap(lambda x: x)
num_rdd2.max(),num_rdd2.min(), num_rdd2.sum(),num_rdd2.variance(),num_rdd2.stdev()
print(num_rdd2.max())
print(num_rdd2.min())
print(num_rdd2.mean())
print(num_rdd2.stats())
