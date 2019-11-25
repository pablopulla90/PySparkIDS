from PySpark.sql import SparkSession
from PySpark_tests_modules import
encode_text_dummy,
encode_text_index,
encode_numeric_zscore,

#Nombre de la Aplicación que se enviará al YARN
appName = "Pre-Procesamiento-UGR16"

#Ruta de donde se cargan los datos para procesarlos
path = "hdfs: /FileStore/com.databricks.spark.csv/newJulioredux3.csv"

#Crea la Sesión de Spark
spark = SparkSession.builder \.appName(appName) \ .getOrCreate()

#Creación del df con los datos del HDFS
df = spark.read.format("csv").option("inferSchema", True)
 .option("header", True).load(path)
df.drop(['te'], axis=1, inplace=True) #Se elimina el atributo TimeStamp del dataframe
encode_text_index(df, 'td')  		  #Funciones Codificación
encode_text_index(df, 'sa')
encode_text_index(df, 'da')
encode_numeric_zscore(df, 'sp')
encode_numeric_zscore(df, 'dp')
encode_text_index(df, 'pr')
encode_text_index(df, 'flg')
encode_text_index(df, 'fwd')
encode_numeric_zscore(df, 'stos')
encode_numeric_zscore(df, 'pkt')
encode_numeric_zscore(df, 'byt')
outcomes = encode_text_index(df, 'norm')
num_classes = len(outcomes)
display(df.head())