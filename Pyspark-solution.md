# ESEMPIO PYSPARK
https://www.youtube.com/watch?v=639JCua-bQU

Dato un dataframe df
...

print(df.columns))

## ORDERBY + LIMIT + TOPANDAS
df.orderBy('views', ascending='False')\
   .limit(10)\
   .toPandas()['field1', 'field2', 'views']

## GROUPBY + AGG + SELECT + ALIAS + ORDERBY
from pyspark.sql.functions import col

df.groupBy('year').agg('views':'sum')\
  .select(col('yr').alias('anno'))\
  .orderBy('year')
  
 ## MATPLOTLIB
 from matplotlib import pyplot as plt
 
 df= df.toPandas()
 
 plt.plot(df.anno)
 
 plt.xlabel('Anni')
 
 plt.title('Titolo')
 
 
 # Esempio 2
 https://www.youtube.com/watch?v=qYis56u8w4U
 
 ## count + filter
 df.filter(df.year > 2020).count()
 
 ## max min
 from pyspark.sql.functions import max, min
 
 df.select ([min ('year')]).show()
