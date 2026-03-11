# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "816aa9ea-ce74-4745-9ac7-5561a71efbc2",
# META       "default_lakehouse_name": "lkh_lab_test",
# META       "default_lakehouse_workspace_id": "b61d0a50-7efb-472c-b6ce-c5db3bda93ab",
# META       "known_lakehouses": [
# META         {
# META           "id": "816aa9ea-ce74-4745-9ac7-5561a71efbc2"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/orders/2019.csv")
# df now is a Spark DataFrame containing CSV data from "Files/orders/2019.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *

schema = StructType([
    StructField("SalesOrderNumber", StringType()),
    StructField("SalesOrderLineNumber", IntegerType()),
    StructField("OrderDate", DateType()),
    StructField("CustomerName", StringType()),
    StructField("Email", StringType()),
    StructField("Item", StringType()),
    StructField("Quantity", IntegerType()),
    StructField("UnitPrice", FloatType()),
    StructField("Tax", FloatType())
])

df = spark.read.format("csv").schema(schema).load("Files/orders/2019.csv")

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales_schema = StructType(
    [
        StructField("SalesOrderNumber", StringType()),
        StructField("SalesOrderLineNumber", IntegerType()),
        StructField("OrderDate", DateType()),
        StructField("CustomerName", StringType()),
        StructField("Email", StringType()),
        StructField("Item", StringType()),
        StructField("Quantity", IntegerType()),
        StructField("UnitPrice", FloatType()),
        StructField("Tax", FloatType())
    ]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales_order = spark.read.format("csv").option("header",False).schema(sales_schema).load("Files/orders/*.csv")
display(sales_order)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customers = sales_order["CustomerName","Email"]
print(customers.count())
print(customers.distinct().count())
customers.distinct()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customers = sales_order.select("CustomerName","Email")
display(customers)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customers = sales_order.select("CustomerName", "Email").where(sales_order['Item']=='Mountain-100 Silver, 44')
display(customers)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

productSales = sales_order.select("Item", "Quantity").groupBy("Item").sum()
display(productSales)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *

yearlySales = sales_order.select(year(col("OrderDate")).alias("Year")).groupBy("Year").count().orderBy("Year")

display(yearlySales)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *


# Create Year and Month columns

transformed_df = sales_order.withColumn("Year", year(col("OrderDate"))).withColumn("Month", month(col("OrderDate")))


# Create the new FirstName and LastName fields

transformed_df = transformed_df.withColumn("FirstName", split(col("CustomerName"), " ").getItem(0)).withColumn("LastName", split(col("CustomerName"), " ").getItem(1))


# Filter and reorder columns

transformed_df = transformed_df["SalesOrderNumber", "SalesOrderLineNumber", "OrderDate", "Year", "Month", "FirstName", "LastName", "Email", "Item", "Quantity", "UnitPrice", "Tax"]


# Display the first five orders

display(transformed_df.limit(5))

transformed_df.write.mode("overwrite").parquet('Files/transformed_data/orders')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

orders_df = spark.read.format("parquet").load("Files/transformed_data/orders")

display(orders_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

orders_df.write.partitionBy("Year","Month").mode("overwrite").parquet("Files/partitioned_data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

orders_2021_df = spark.read.format("parquet").load("Files/partitioned_data/Year=2021/Month=*")


display(orders_2021_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales_order.write.format("delta").mode("overwrite").saveAsTable("salesorders")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("DESCRIBE EXTENDED salesorders").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lkh_lab_test.dbo.salesorders LIMIT 1000")


display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT YEAR(OrderDate) AS OrderYear,
# MAGIC 
# MAGIC SUM((UnitPrice * Quantity) + Tax) AS GrossRevenue
# MAGIC 
# MAGIC FROM lkh_lab_test.dbo.salesorders
# MAGIC 
# MAGIC GROUP BY YEAR(OrderDate)
# MAGIC 
# MAGIC ORDER BY OrderYear;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT * FROM salesorders

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sqlQuery = "SELECT CAST(YEAR(OrderDate) AS CHAR(4)) AS OrderYear, \
SUM((UnitPrice * Quantity) + Tax) AS GrossRevenue, \
COUNT(DISTINCT SalesOrderNumber) AS YearlyCounts \
FROM salesorders \
GROUP BY CAST(YEAR(OrderDate) AS CHAR(4)) \
ORDER BY OrderYear"

df_spark = spark.sql(sqlQuery)

df_spark.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from matplotlib import pyplot as plt


# matplotlib requires a Pandas dataframe, not a Spark one

df_sales = df_spark.toPandas()


# Create a bar plot of revenue by year

plt.bar(x=df_sales['OrderYear'], height=df_sales['GrossRevenue'])


# Display the plot

plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from matplotlib import pyplot as plt


# Clear the plot area

plt.clf()


# Create a bar plot of revenue by year

plt.bar(x=df_sales['OrderYear'], height=df_sales['GrossRevenue'], color='orange')


# Customize the chart

plt.title('Revenue by Year')

plt.xlabel('Year')

plt.ylabel('Revenue')

plt.grid(color='#95a5a6', linestyle='--', linewidth=2, axis='y', alpha=0.7)

plt.xticks(rotation=45)


# Show the figure

plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from matplotlib import pyplot as plt


# Clear the plot area

plt.clf()


# Create a Figure

fig = plt.figure(figsize=(8,3))


# Create a bar plot of revenue by year

plt.bar(x=df_sales['OrderYear'], height=df_sales['GrossRevenue'], color='orange')


# Customize the chart

plt.title('Revenue by Year')

plt.xlabel('Year')

plt.ylabel('Revenue')

plt.grid(color='#95a5a6', linestyle='--', linewidth=2, axis='y', alpha=0.7)

plt.xticks(rotation=45)


# Show the figure

plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from matplotlib import pyplot as plt


# Clear the plot area

plt.clf()


# Create a figure for 2 subplots (1 row, 2 columns)

fig, ax = plt.subplots(1, 2, figsize = (10,4))


# Create a bar plot of revenue by year on the first axis

ax[0].bar(x=df_sales['OrderYear'], height=df_sales['GrossRevenue'], color='orange')

ax[0].set_title('Revenue by Year')


# Create a pie chart of yearly order counts on the second axis

ax[1].pie(df_sales['YearlyCounts'])

ax[1].set_title('Orders per Year')

ax[1].legend(df_sales['OrderYear'])


# Add a title to the Figure

fig.suptitle('Sales Data')


# Show the figure

plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import seaborn as sns


# Clear the plot area

plt.clf()


# Create a bar chart

ax = sns.barplot(x="OrderYear", y="GrossRevenue", data=df_sales)


plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import seaborn as sns


# Clear the plot area

plt.clf()


# Set the visual theme for seaborn

sns.set_theme(style="whitegrid")


# Create a bar chart

ax = sns.barplot(x="OrderYear", y="GrossRevenue", data=df_sales)


plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import seaborn as sns


# Clear the plot area

plt.clf()


# Create a line chart

ax = sns.lineplot(x="OrderYear", y="GrossRevenue", data=df_sales)


plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
