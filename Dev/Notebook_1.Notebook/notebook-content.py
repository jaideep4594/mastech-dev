# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ac7dea75-06ca-42c3-a267-2137f4a0c600",
# META       "default_lakehouse_name": "sales",
# META       "default_lakehouse_workspace_id": "b61d0a50-7efb-472c-b6ce-c5db3bda93ab",
# META       "known_lakehouses": [
# META         {
# META           "id": "ac7dea75-06ca-42c3-a267-2137f4a0c600"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.types import *


# Create the schema for the table

orderSchema = StructType([

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


# Import all files from bronze folder of lakehouse

df = spark.read.format("csv").option("header", "false").schema(orderSchema).load("Files/bronze/*.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.head(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import when, lit, col, current_timestamp, input_file_name


# Add columns IsFlagged, CreatedTS and ModifiedTS

df = df.withColumn("FileName", input_file_name()) \
.withColumn("IsFlagged", when(col("OrderDate") < '2019-08-01',True).otherwise(False)) \
.withColumn("CreatedTS", current_timestamp()).withColumn("ModifiedTS", current_timestamp())


# Update CustomerName to "Unknown" if CustomerName null or empty

df = df.withColumn("CustomerName", when((col("CustomerName").isNull() | (col("CustomerName")=="")),lit("Unknown")).otherwise(col("CustomerName")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *

from delta.tables import *


DeltaTable.createIfNotExists(spark) \
.tableName("sales.sales_silver") \
.addColumn("SalesOrderNumber", StringType()) \
.addColumn("SalesOrderLineNumber", IntegerType()) \
.addColumn("OrderDate", DateType()) \
.addColumn("CustomerName", StringType()) \
.addColumn("Email", StringType()) \
.addColumn("Item", StringType()) \
.addColumn("Quantity", IntegerType()) \
.addColumn("UnitPrice", FloatType()) \
.addColumn("Tax", FloatType()) \
.addColumn("FileName", StringType()) \
.addColumn("IsFlagged", BooleanType()) \
.addColumn("CreatedTS", DateType()) \
.addColumn("ModifiedTS", DateType()) \
.execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import *
deltaTable = DeltaTable.forPath(spark, 'Tables/sales_silver')
dfUpdates = df
deltaTable.alias('silver') \
.merge(
dfUpdates.alias('updates'),
'silver.SalesOrderNumber = updates.SalesOrderNumber and silver.OrderDate = updates.OrderDate and silver.CustomerName = updates.CustomerName and silver.Item = updates.Item'
) \
.whenMatchedUpdate(set =
{
}
) \
.whenNotMatchedInsert(values =
{
"SalesOrderNumber": "updates.SalesOrderNumber",
"SalesOrderLineNumber": "updates.SalesOrderLineNumber",
"OrderDate": "updates.OrderDate",
"CustomerName": "updates.CustomerName",
"Email": "updates.Email",
"Item": "updates.Item",
"Quantity": "updates.Quantity",
"UnitPrice": "updates.UnitPrice",
"Tax": "updates.Tax",
"FileName": "updates.FileName",
"IsFlagged": "updates.IsFlagged",
"CreatedTS": "updates.CreatedTS",
"ModifiedTS": "updates.ModifiedTS"
}
) \
.execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *

from delta.tables import*


# Define the schema for the dimdate_gold table

DeltaTable.createIfNotExists(spark) \
.tableName("sales.dimdate_gold") \
.addColumn("OrderDate", DateType()) \
.addColumn("Day", IntegerType()) \
.addColumn("Month", IntegerType()) \
.addColumn("Year", IntegerType()) \
.addColumn("mmmyyyy", StringType()) \
.addColumn("yyyymm", StringType()) \
.execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
