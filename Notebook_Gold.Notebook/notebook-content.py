# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "73ac2635-2255-4ca8-80b3-f0fc5c54d984",
# META       "default_lakehouse_name": "Ecom_Lakehouse",
# META       "default_lakehouse_workspace_id": "b57a8cf2-69de-45ab-b9cd-550edbbdc0d1",
# META       "known_lakehouses": [
# META         {
# META           "id": "73ac2635-2255-4ca8-80b3-f0fc5c54d984"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 1 Build Gold Dimensions

# CELL ********************

from pyspark.sql.functions import col, monotonically_increasing_id

# Define source tables and surrogate keys for all Gold dimensions
dimension_config = {
    "dim_customer": {"source": "silver.dim_customers", "key": "CustomerKey"},
    "dim_product": {"source": "silver.dim_products", "key": "ProductKey"},
    "dim_employee": {"source": "silver.employees", "key": "EmployeeKey"},
    "dim_category": {"source": "silver.categories", "key": "CategoryKey"},
    "dim_supplier": {"source": "silver.suppliers", "key": "SupplierKey"},
    "dim_shipper": {"source": "silver.shippers", "key": "ShipperKey"},
    "dim_territory": {"source": "silver.territories", "key": "TerritoryKey"}
}

for dimension_name, config in dimension_config.items():

    key = config["key"]

    # Read Silver dimension and add a surrogate key
    df = spark.read.table(config["source"]).withColumn(
        key, monotonically_increasing_id()
    )

    # Move surrogate key to the first column
    df = df.select(key, *[c for c in df.columns if c != key])

    # Save dimension as a Delta table in Gold
    df.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"gold.{dimension_name}")

    # Validate surrogate key integrity
    final_df = spark.read.table(f"gold.{dimension_name}")

    null_keys = final_df.filter(col(key).isNull()).count()

    duplicate_keys = (
        final_df.groupBy(key)
        .count()
        .filter(col("count") > 1)
        .count()
    )

    if null_keys > 0:
        raise Exception(f"{dimension_name}: NULL surrogate keys detected.")

    if duplicate_keys > 0:
        raise Exception(f"{dimension_name}: Duplicate surrogate keys detected.")

print("✅ Gold Dimensions completed successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 2️ Gold Dim Date

# CELL ********************

from pyspark.sql.functions import (
    col, min, max, sequence, explode,
    year, quarter, month, weekofyear,
    dayofmonth, dayofweek, date_format, when
)

SOURCE_TABLE = "silver.orders"
TARGET_TABLE = "gold.dim_date"

# Get the date range from orders
orders = spark.read.table(SOURCE_TABLE)

date_range = orders.select(
    min("OrderDate").alias("MinDate"),
    max("OrderDate").alias("MaxDate")
    #select  row  from  spark to  drive
).collect()[0]

min_date = date_range["MinDate"]
max_date = date_range["MaxDate"]

# Generate one row for every date between MinDate and MaxDate
dates = (
    spark.createDataFrame(
        [(min_date, max_date)],
        ["MinDate", "MaxDate"]
    )
    .select(
        explode(
            sequence(col("MinDate"), col("MaxDate"))
        ).alias("FullDate")
    )
)

# Build Date Dimension attributes
dim_date = (
    dates
    .withColumn("DateKey", date_format("FullDate", "yyyyMMdd").cast("int"))
    .withColumn("Year", year("FullDate"))
    .withColumn("Quarter", quarter("FullDate"))
    .withColumn("Month", month("FullDate"))
    .withColumn("MonthName", date_format("FullDate", "MMMM"))
    .withColumn("MonthNumber", month("FullDate"))
    .withColumn("WeekOfYear", weekofyear("FullDate"))
    .withColumn("Day", dayofmonth("FullDate"))
    .withColumn("DayName", date_format("FullDate", "EEEE"))
    .withColumn("DayOfWeek", dayofweek("FullDate"))
    .withColumn(
        "IsWeekend",
        when(dayofweek("FullDate").isin(1, 7), True).otherwise(False)
    )
    .select(
        "DateKey", "FullDate", "Year", "Quarter",
        "Month", "MonthName", "MonthNumber",
        "WeekOfYear", "Day", "DayName",
        "DayOfWeek", "IsWeekend"
    )
)

# Save Date Dimension to Gold
dim_date.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_TABLE)

# Validate the saved dimension
final_date = spark.read.table(TARGET_TABLE)

total_rows = final_date.count()

null_keys = final_date.filter(
    col("DateKey").isNull()
).count()

duplicate_keys = (
    final_date.groupBy("DateKey")
    .count()
    .filter(col("count") > 1)
    .count()
)

if total_rows == 0:
    raise Exception("dim_date contains zero rows.")

if null_keys > 0:
    raise Exception("dim_date contains NULL DateKey values.")

if duplicate_keys > 0:
    raise Exception("dim_date contains duplicate DateKey values.")

print("✅ Gold Dim Date completed successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 1 Fact Sales


# CELL ********************

from pyspark.sql.functions import col

# Load Silver tables
order_details = spark.read.table("silver.order_details")
orders = spark.read.table("silver.orders")

# Load Gold dimensions
dim_customer = (
    spark.read.table("gold.dim_customer")
    .filter(col("IsCurrent") == True)
)

dim_product = spark.read.table("gold.dim_product").filter(col("IsCurrent") == True)
dim_supplier = spark.read.table("gold.dim_supplier")
dim_employee = spark.read.table("gold.dim_employee")
dim_shipper = spark.read.table("gold.dim_shipper")
dim_date = spark.read.table("gold.dim_date")

# Build Fact Sales joins
fact_df = (
    order_details.alias("od")
    .join(
        orders.alias("o"),
        col("od.OrderID") == col("o.OrderID"),
        "inner"
    )
    .join(
        dim_customer.alias("c"),
        col("o.CustomerID") == col("c.CustomerID"),
        "left"
    )
    .join(
        dim_product.alias("p"),
        col("od.ProductID") == col("p.ProductID"),
        "left"
    )
    .join(
        dim_supplier.alias("sup"),
        col("p.SupplierID") == col("sup.SupplierID"),
        "left"
    )
    .join(
        dim_employee.alias("e"),
        col("o.EmployeeID") == col("e.EmployeeID"),
        "left"
    )
    .join(
        dim_shipper.alias("s"),
        col("o.ShipVia") == col("s.ShipperID"),
        "left"
    )
    .join(
        dim_date.alias("d"),
        col("o.OrderDate") == col("d.FullDate"),
        "left"
    )
)

print("✅ All Fact Sales joins completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 2 Build  The Fact  Table
#  

# CELL ********************

from pyspark.sql.functions import col, monotonically_increasing_id, round

# Select required columns and dimension surrogate keys
fact_sales = (
    fact_df
    .select(
        # Business identifiers
        col("od.OrderID").alias("OrderID"),
        col("od.ProductID").alias("ProductID"),

        # Dimension surrogate keys
        col("c.CustomerKey").alias("CustomerKey"),
        col("p.ProductKey").alias("ProductKey"),
        col("sup.SupplierKey").alias("SupplierKey"),
        col("e.EmployeeKey").alias("EmployeeKey"),
        col("s.ShipperKey").alias("ShipperKey"),
        col("d.DateKey").alias("DateKey"),

        # Sales attributes
        col("od.UnitPrice").alias("UnitPrice"),
        col("od.Quantity").alias("Quantity"),
        col("od.Discount").alias("Discount"),
        col("o.Freight").alias("Freight")
    )

    # Calculate sales metrics
    .withColumn(
        "GrossSales",
        round(col("UnitPrice") * col("Quantity"), 2)
    )
    .withColumn(
        "DiscountAmount",
        round(col("GrossSales") * col("Discount"), 2)
    )
    .withColumn(
        "NetSales",
        round(col("GrossSales") - col("DiscountAmount"), 2)
    )

    # Create Fact surrogate key
    .withColumn(
        "FactSalesKey",
        monotonically_increasing_id()
    )

    # Final column order
    .select(
        "FactSalesKey",
        "OrderID",
        "ProductID",
        "CustomerKey",
        "ProductKey",
        "SupplierKey",
        "EmployeeKey",
        "ShipperKey",
        "DateKey",
        "UnitPrice",
        "Quantity",
        "Discount",
        "Freight",
        "GrossSales",
        "DiscountAmount",
        "NetSales"
    )
)
# Save final Fact Sales table

fact_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.fact_sales")

print("✅ gold.fact_sales saved successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Fact  Validation


# CELL ********************

from pyspark.sql.functions import col, abs as spark_abs

fact_count = fact_sales.count()

# Validate fact row count
expected_count = fact_df.count()

if fact_count != expected_count:
    raise Exception(f"Expected {expected_count} fact rows, found {fact_count}")

# Validate FactSalesKey
null_fact_keys = fact_sales.filter(
    col("FactSalesKey").isNull()
).count()

duplicate_fact_keys = (
    fact_sales
    .groupBy("FactSalesKey")
    .count()
    .filter(col("count") > 1)
    .count()
)

if null_fact_keys > 0:
    raise Exception("NULL FactSalesKey detected.")

if duplicate_fact_keys > 0:
    raise Exception("Duplicate FactSalesKey detected.")

# Validate fact grain
duplicate_grain = (
    fact_sales
    .groupBy("OrderID", "ProductID")
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicate_grain > 0:
    raise Exception("Duplicate OrderID + ProductID grain detected.")

# Validate foreign keys
foreign_keys = [
    "CustomerKey",
    "ProductKey",
    "EmployeeKey",
    "ShipperKey",
    "SupplierKey",
    "DateKey"
]

for key in foreign_keys:
    if fact_sales.filter(col(key).isNull()).count() > 0:
        raise Exception(f"NULL foreign key detected: {key}")

# Validate business columns
business_columns = [
    "OrderID",
    "ProductID",
    "UnitPrice",
    "Quantity",
    "Discount",
    "GrossSales",
    "DiscountAmount",
    "NetSales"
]

for column_name in business_columns:
    if fact_sales.filter(col(column_name).isNull()).count() > 0:
        raise Exception(f"NULL business value detected: {column_name}")

# Validate sales calculations
gross_errors = fact_sales.filter(
    spark_abs(
        col("GrossSales") - (col("UnitPrice") * col("Quantity"))
    ) > 0.01
).count()

if gross_errors > 0:
    raise Exception("GrossSales calculation error detected.")

discount_errors = fact_sales.filter(
    spark_abs(
        col("DiscountAmount") - (col("GrossSales") * col("Discount"))
    ) > 0.01
).count()

if discount_errors > 0:
    raise Exception("DiscountAmount calculation error detected.")

net_sales_errors = fact_sales.filter(
    spark_abs(
        col("NetSales") - (col("GrossSales") - col("DiscountAmount"))
    ) > 0.01
).count()

if net_sales_errors > 0:
    raise Exception("NetSales calculation error detected.")

# Validate business rules
if fact_sales.filter(col("NetSales") < 0).count() > 0:
    raise Exception("Negative NetSales detected.")

if fact_sales.filter(
    (col("Discount") < 0) | (col("Discount") > 1)
).count() > 0:
    raise Exception("Invalid Discount value detected.")

print("✅ Fact Sales validation passed successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
