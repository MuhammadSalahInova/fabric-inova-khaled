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
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
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

# CELL ********************

from pyspark.sql.functions import trim, col, to_date


# Define Silver transformation rules
table_config = {
    "customers": {
        "key_columns": ["CustomerID"],
        "string_cols": [
            "CustomerID", "CompanyName", "ContactName", "ContactTitle",
            "Address", "City", "Region", "PostalCode", "Country",
            "Phone", "Fax", "Operation"
        ],
        "date_cols": [],
        "timestamp_cols": ["LoadDate", "CreatedDate", "ModifiedDate"],
        "numeric_cols": {},
        "boolean_cols": {},
        "drop_cols": [],
        "not_null_cols": ["CustomerID"]
    },
    "orders": {
        "key_columns": ["OrderID"],
        "string_cols": [
            "CustomerID", "ShipName", "ShipAddress", "ShipCity",
            "ShipRegion", "ShipPostalCode", "ShipCountry", "Operation"
        ],
        "date_cols": ["OrderDate", "RequiredDate", "ShippedDate"],
        "timestamp_cols": ["LoadDate", "CreatedDate", "ModifiedDate"],
        "numeric_cols": {
            "OrderID": "long", "EmployeeID": "long",
            "ShipVia": "long", "Freight": "double"
        },
        "boolean_cols": {},
        "drop_cols": [],
        "not_null_cols": ["OrderID", "CustomerID"]
    },
    "order_details": {
        "key_columns": ["OrderID", "ProductID"],
        "string_cols": ["Operation"],
        "date_cols": [],
        "timestamp_cols": ["LoadDate", "CreatedDate", "ModifiedDate"],
        "numeric_cols": {
            "OrderID": "long", "ProductID": "long",
            "UnitPrice": "double", "Quantity": "long", "Discount": "double"
        },
        "boolean_cols": {},
        "drop_cols": [],
        "not_null_cols": ["OrderID", "ProductID"]
    },
    "products": {
        "key_columns": ["ProductID"],
        "string_cols": ["ProductName", "QuantityPerUnit", "Operation"],
        "date_cols": [],
        "timestamp_cols": ["LoadDate", "CreatedDate", "ModifiedDate"],
        "numeric_cols": {
            "ProductID": "long", "SupplierID": "long", "CategoryID": "long",
            "UnitPrice": "double", "UnitsInStock": "long",
            "UnitsOnOrder": "long", "ReorderLevel": "long"
        },
        "boolean_cols": {"Discontinued": "boolean"},
        "drop_cols": [],
        "not_null_cols": ["ProductID", "ProductName"]
    },
    "employees": {
        "key_columns": ["EmployeeID"],
        "string_cols": [
            "LastName", "FirstName", "Title", "TitleOfCourtesy",
            "Address", "City", "Region", "PostalCode", "Country",
            "HomePhone", "Extension", "Notes", "PhotoPath"
        ],
        "date_cols": ["BirthDate", "HireDate"],
        "timestamp_cols": [],
        "numeric_cols": {"EmployeeID": "long", "ReportsTo": "long"},
        "boolean_cols": {},
        "drop_cols": ["Photo"],
        "not_null_cols": ["EmployeeID"]
    },
    "categories": {
        "key_columns": ["CategoryID"],
        "string_cols": ["CategoryName", "Description"],
        "date_cols": [],
        "timestamp_cols": [],
        "numeric_cols": {"CategoryID": "long"},
        "boolean_cols": {},
        "drop_cols": ["Picture"],
        "not_null_cols": ["CategoryID"]
    },
    "shippers": {
        "key_columns": ["ShipperID"],
        "string_cols": ["CompanyName", "Phone"],
        "date_cols": [],
        "timestamp_cols": [],
        "numeric_cols": {"ShipperID": "long"},
        "boolean_cols": {},
        "drop_cols": [],
        "not_null_cols": ["ShipperID"]
    },
    "suppliers": {
        "key_columns": ["SupplierID"],
        "string_cols": [
            "CompanyName", "ContactName", "ContactTitle", "Address",
            "City", "Region", "PostalCode", "Country", "Phone",
            "Fax", "HomePage"
        ],
        "date_cols": [],
        "timestamp_cols": [],
        "numeric_cols": {"SupplierID": "long"},
        "boolean_cols": {},
        "drop_cols": [],
        "not_null_cols": ["SupplierID"]
    },
    "territories": {
        "key_columns": ["TerritoryID"],
        "string_cols": ["TerritoryID", "TerritoryDescription"],
        "date_cols": [],
        "timestamp_cols": [],
        "numeric_cols": {"RegionID": "long"},
        "boolean_cols": {},
        "drop_cols": [],
        "not_null_cols": ["TerritoryID"]
    }
}


# Clean and validate Silver table
def clean_table(table_name):

    config = table_config[table_name]
    bronze_table = f"bronze_{table_name}"
    silver_table = f"silver.{table_name}"

    print(f"\n{'=' * 80}\nPROCESSING: {table_name}\n{'=' * 80}")

    # Read Bronze table
    try:
        df = spark.read.table(bronze_table)
    except Exception as e:
        raise Exception(f"Cannot read {bronze_table}: {str(e)}")

    rows_before = df.count()
    print(f"Rows before transformation: {rows_before}")

    # Drop unwanted columns
    drop_cols = [c for c in config["drop_cols"] if c in df.columns]

    if drop_cols:
        df = df.drop(*drop_cols)
        print(f"🧹 Dropped columns: {drop_cols}")
    else:
        print("✅ No unwanted columns to drop")

    # Clean string columns
    for c in config["string_cols"]:
        if c in df.columns:
            df = df.withColumn(c, trim(col(c)))

    print("✅ String columns cleaned")

    # Convert date columns
    for c in config["date_cols"]:
        if c not in df.columns:
            raise Exception(f"{table_name}: Required date column missing: {c}")
        df = df.withColumn(c, to_date(col(c)))

    print("✅ Date columns converted")

    # Convert timestamp columns
    for c in config["timestamp_cols"]:
        if c not in df.columns:
            raise Exception(f"{table_name}: Required timestamp column missing: {c}")
        df = df.withColumn(c, col(c).cast("timestamp"))

    print(
        "✅ Timestamp columns converted"
        if config["timestamp_cols"]
        else "✅ No timestamp columns required"
    )

    # Convert and validate numeric columns
    for c, data_type in config["numeric_cols"].items():

        if c not in df.columns:
            raise Exception(f"{table_name}: Required numeric column missing: {c}")

        nulls_before_cast = df.filter(col(c).isNull()).count()
        df = df.withColumn(c, col(c).cast(data_type))
        nulls_after_cast = df.filter(col(c).isNull()).count()
        new_nulls = nulls_after_cast - nulls_before_cast

        if new_nulls > 0:
            raise Exception(
                f"{table_name}: Column '{c}' produced {new_nulls} new NULL values "
                f"after casting to {data_type}."
            )

    print("✅ Numeric columns converted and validated")

    # Convert boolean columns
    for c, data_type in config["boolean_cols"].items():

        if c not in df.columns:
            raise Exception(f"{table_name}: Required boolean column missing: {c}")

        df = df.withColumn(c, col(c).cast(data_type))

    print("✅ Boolean columns converted")

    # Validate required columns
    missing_required = [
        c for c in config["not_null_cols"]
        if c not in df.columns
    ]

    if missing_required:
        raise Exception(
            f"{table_name}: Missing required columns: {missing_required}"
        )

    # Validate required values
    null_counts = {}

    for c in config["not_null_cols"]:
        count_nulls = df.filter(col(c).isNull()).count()
        if count_nulls > 0:
            null_counts[c] = count_nulls

    if null_counts:
        raise Exception(
            f"{table_name}: Required columns contain NULL values: {null_counts}"
        )

    print("✅ Required columns contain no NULL values")

    # Validate business keys
    key_columns = config["key_columns"]
    missing_keys = [c for c in key_columns if c not in df.columns]

    if missing_keys:
        raise Exception(
            f"{table_name}: Missing business key columns: {missing_keys}"
        )

    null_key_condition = None

    for c in key_columns:
        condition = col(c).isNull()
        null_key_condition = (
            condition
            if null_key_condition is None
            else null_key_condition | condition
        )

    null_key_count = df.filter(null_key_condition).count()

    if null_key_count > 0:
        raise Exception(
            f"{table_name}: {null_key_count} NULL business-key rows."
        )

    # Validate duplicate business keys
    total_rows = df.count()
    distinct_key_rows = df.select(*key_columns).dropDuplicates().count()
    duplicate_count = total_rows - distinct_key_rows

    print(f"Business key duplicates: {duplicate_count}")

    if duplicate_count > 0:
        raise Exception(
            f"{table_name} contains {duplicate_count} duplicate business-key rows."
        )

    # Validate Operation values
    if "Operation" in df.columns:

        invalid_operations = (
            df.filter(
                col("Operation").isNotNull()
                & ~col("Operation").isin("I", "U")
            )
            .select("Operation")
            .distinct()
            .collect()
        )

        if invalid_operations:
            invalid_values = [row["Operation"] for row in invalid_operations]
            raise Exception(
                f"{table_name}: Invalid Operation values: {invalid_values}. "
                f"Expected only I or U."
            )

        print("✅ Operation values validated: I / U")

    else:
        print("✅ Operation column not applicable")

    # Create DataFrame checkpoint
    df = df.localCheckpoint(eager=True)
    print("✅ DataFrame checkpoint created")

    # Save Silver table
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(silver_table)
    )

    # Validate final row count
    rows_after = spark.read.table(silver_table).count()

    print(f"Rows after transformation: {rows_after}")
    print(f"Duplicates: {duplicate_count}")

    if rows_before != rows_after:
        raise Exception(
            f"{table_name}: Row count changed: {rows_before} -> {rows_after}"
        )

    print(f"✅ Saved successfully: {silver_table}")


# Process all Silver tables
for table_name in table_config:
    clean_table(table_name)

# Print final transformation status
print(f"\n{'=' * 80}")
print("✅ SILVER TRANSFORMATION COMPLETED SUCCESSFULLY")
print(f"{'=' * 80}")
print(f"Tables processed: {len(table_config)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
