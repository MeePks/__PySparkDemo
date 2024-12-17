# Initialize Spark session
from pyspark.sql import SparkSession
#from pyspark.sql.functions import max
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()


#Load csv files into datafrmes
df_customer=spark.read.format('csv').option('header', True).load('/samples/customers.csv')
df_sales=spark.read.format('csv').option('header', True).load('/samples/sales.csv')
df_product=spark.read.format('csv').option('header', True).load('/samples/products.csv')
df_sales=df_sales.withColumn("total_amount",df_sales['total_amount'].cast("float"))
df_sales=df_sales.withColumn("sale_date",df_sales['sale_date'].cast("date"))
df_sales.printSchema()
#selecting specific columns 
df_customer.select("customer_id","first_name").show(1)

#filtering dataframe
df_customer.filter("customer_id==1 OR first_name=='Emma'").show()

#group by
sales=df_sales.groupBy("customer_id").max("total_amount")
sales=sales.withColumnRenamed("max(total_amount)","total_sales")
sales=df_sales.groupBy("customer_id").agg({"total_amount": "sum"})
sales.show()

#join
df_customer.join(df_sales, "customer_id", how='inner').show()

df_customer.join(df_sales, "customer_id", how="left")\
.filter(df_sales['customer_id'].isNull())\
.select(df_customer['customer_id']).show()



