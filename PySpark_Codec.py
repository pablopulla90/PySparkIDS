from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('cars.csv')
df.select('year', 'model').write.format('com.databricks.spark.csv').save('newcars.csv')


from pyspark.sql import SQLContext
from pyspark.sql.types import *

sqlContext = SQLContext(sc)
customSchema = StructType([ \
    StructField("Duration", IntegerType(), True), \
    StructField("Src_IP", StringType(), True), \
    StructField("Dst_IP", StringType(), True), \
    StructField("Src_Port", StringType(), True), \
    StructField("Dest_Port", StringType(), True), \
    StructField("Proto", StringType(), True), \
    StructField("Flags", StringType(), True), \
    StructField("Forward_Status", IntegerType(), True), \
    StructField("Number_of_Packets", IntegerType(), True), \
    StructField("Forward_Status", IntegerType(), True), \
    StructField("Bytes", IntegerType(), True)])

df.loc[df.Result != 'background', 'Result'] = 1
            df.loc[df.Result == 'background', 'Result'] = 0
            count+=1
            df.to_csv(outpath+'/out'+str(count)+'.csv', header=False, index=False)
            
df = sqlContext.read \
    .format('com.databricks.spark.csv') \
    .options(header='true') \
    .load('./julioredux3.csv', schema = customSchema)

df.select('Duration', 'Src_IP', 'Dst_IP', 'Src_Port', 'Dest_Port', 'Proto', 'Flags', 'Forward_Status', 'Number_of_Packets', 'Bytes').write \
    .format('com.databricks.spark.csv') \
    .save('./newjulioredux3.csv')