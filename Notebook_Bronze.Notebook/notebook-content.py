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

# Define Bronze tables
tables = [
    "customers", "orders", "order_details", "products", "employees",
    "categories", "shippers", "suppliers", "territories"
]

errors = []

# Process Bronze tables
for table_name in tables:

    table = f"bronze_{table_name}"

    try:
        print(f"\n{'=' * 80}\nPROCESSING: {table}\n{'=' * 80}")

        # Read Bronze table
        df = spark.read.table(table)
        rows_before = df.count()
        print(f"Rows before cleanup: {rows_before}")

        # Remove OData metadata
        columns_to_drop = [
            c for c in df.columns
            if c.startswith("@odata.") or c.endswith(".@odata.etag")
        ]

        if columns_to_drop:
            df = df.drop(*columns_to_drop)
            print(f"🧹 Removed OData columns: {columns_to_drop}")
        else:
            print("✅ No OData metadata found")

        # Remove value. prefix
        renamed_columns = [
            c[len("value."):] if c.startswith("value.") else c
            for c in df.columns
        ]

        if renamed_columns != df.columns:
            df = df.toDF(*renamed_columns)
            print("🧹 Removed 'value.' prefix")
        else:
            print("✅ No 'value.' prefix found")

        # Validate column names
        duplicate_columns = [
            c for c in set(df.columns)
            if df.columns.count(c) > 1
        ]

        if duplicate_columns:
            raise Exception(
                f"{table} contains duplicate column names: {duplicate_columns}"
            )

        print("✅ Column names are unique")

        # Save cleaned Bronze table
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table)
        )

        # Validate final row count
        rows_after = spark.read.table(table).count()
        print(f"Rows after cleanup: {rows_after}")

        if rows_before != rows_after:
            raise Exception(
                f"Row count changed during cleanup: {rows_before} -> {rows_after}"
            )

        print(f" {table} cleaned successfully")

    except Exception as e:
        error_message = f"{table}: PROCESSING FAILED - {str(e)}"
        print(f" {error_message}")
        errors.append(error_message)

# Print final cleanup status
print(f"\n{'=' * 80}\nBRONZE CLEANUP SUMMARY\n{'=' * 80}")

if errors:
    print(f" TOTAL ERRORS: {len(errors)}")

    for error in errors:
        print(f"  - {error}")

    raise Exception(f"Bronze cleanup failed with {len(errors)} error(s).")

else:
    print(" ALL BRONZE TABLES CLEANED SUCCESSFULLY.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

