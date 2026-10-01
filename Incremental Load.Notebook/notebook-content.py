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

# CELL ********************

import os
import uuid
import pandas as pd

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, col, lit, row_number, when
from pyspark.sql.window import Window


# Define Excel file and Bronze/History tables
file_path = "/lakehouse/default/Files/incoming/FakeStore_Orders_OrderDetails_Customers_Products_Linked.xlsx"

sheet_config = {
    "Orders": {
        "bronze_table": "bronze_orders",
        "history_table": "bronze_history_orders",
        "key_columns": ["OrderID"]
    },
    "OrderDetails": {
        "bronze_table": "bronze_order_details",
        "history_table": "bronze_history_order_details",
        "key_columns": ["OrderID", "ProductID"]
    },
    "Customers": {
        "bronze_table": "bronze_customers",
        "history_table": "bronze_history_customers",
        "key_columns": ["CustomerID"]
    },
    "Products": {
        "bronze_table": "bronze_products",
        "history_table": "bronze_history_products",
        "key_columns": ["ProductID"]
    }
}

# Check if a new Excel file exists
if not os.path.exists(file_path):
    print("ℹ️ No new Excel file found.")
    print("Incremental Load skipped.")
    mssparkutils.notebook.exit("SKIPPED - No file found")

print("✅ Excel file found.")
print("Starting Incremental Load...")

# Generate batch metadata
batch_id = str(uuid.uuid4())
source_file = os.path.basename(file_path)

for sheet_name, config in sheet_config.items():

    print(f"\n{'=' * 80}\nPROCESSING: {sheet_name}\n{'=' * 80}")

    bronze_table = config["bronze_table"]
    history_table = config["history_table"]
    keys = config["key_columns"]

    # Read Excel sheet
    pdf = pd.read_excel(file_path, sheet_name=sheet_name)

    if pdf.empty:
        print(f"ℹ️ {sheet_name}: Excel sheet is empty.")
        continue

    new_df = spark.createDataFrame(pdf)
    print(f"Excel rows: {new_df.count()}")

    # Validate Operation values
    if "Operation" not in new_df.columns:
        raise Exception(f"{sheet_name}: Operation column is missing from Excel.")

    new_df = new_df.withColumn("Operation", col("Operation").cast("string"))

    invalid_operations = (
        new_df.filter(~col("Operation").isin("I", "U"))
        .select("Operation")
        .distinct()
        .collect()
    )

    if invalid_operations:
        invalid_values = [row["Operation"] for row in invalid_operations]
        raise Exception(
            f"{sheet_name}: Invalid Operation values: {invalid_values}. "
            f"Only I and U are allowed."
        )

    print("✅ Operation values validated: I / U only")

    # Read existing Bronze table
    if not spark.catalog.tableExists(bronze_table):
        raise Exception(f"Bronze table does not exist: {bronze_table}")

    bronze_df = spark.read.table(bronze_table)
    rows_before = bronze_df.count()
    bronze_columns = bronze_df.columns

    print(f"Bronze rows before load: {rows_before}")

    # Validate key columns
    missing_keys = [k for k in keys if k not in new_df.columns]

    if missing_keys:
        raise Exception(f"{sheet_name}: Missing key columns in Excel: {missing_keys}")

    # Remove duplicate Excel records
    if "ModifiedDate" in new_df.columns:
        window_spec = Window.partitionBy(*keys).orderBy(
            col("ModifiedDate").desc_nulls_last()
        )
    else:
        window_spec = Window.partitionBy(*keys).orderBy(
            *[col(k).asc() for k in keys]
        )

    new_df = (
        new_df.withColumn("_row_number", row_number().over(window_spec))
        .filter(col("_row_number") == 1)
        .drop("_row_number")
    )

    print(f"Excel rows after deduplication: {new_df.count()}")

    # Count INSERT and UPDATE operations
    operation_counts = {
        row["Operation"]: row["count"]
        for row in new_df.groupBy("Operation").count().collect()
    }

    insert_count = operation_counts.get("I", 0)
    update_count = operation_counts.get("U", 0)

    print(f"I (INSERT) : {insert_count}")
    print(f"U (UPDATE) : {update_count}")
    print("✅ Excel Operation will be preserved: I / U")

    # Add load timestamp
    new_df = new_df.withColumn("LoadDate", current_timestamp())

    metadata_columns = {
        "LoadDate", "Operation", "CreatedDate", "ModifiedDate",
        "BatchID", "SourceFile", "ProcessedAt"
    }

    business_columns = [
        c for c in new_df.columns
        if c in bronze_columns and c not in metadata_columns
    ]

    # Prepare existing Bronze records
    target_columns = []

    for c in bronze_columns:
        if c in keys or c in business_columns:
            target_columns.append(col(c).alias(f"_target_{c}"))
        elif c == "CreatedDate":
            target_columns.append(col(c).alias("_target_CreatedDate"))
        elif c == "ModifiedDate":
            target_columns.append(col(c).alias("_target_ModifiedDate"))

    existing_df = (
        bronze_df.select(*target_columns)
        .withColumn("_Exists", lit(True))
    )

    # Join Excel records with Bronze
    join_condition = None

    for key in keys:
        condition = new_df[key] == existing_df[f"_target_{key}"]
        join_condition = condition if join_condition is None else join_condition & condition

    enriched_df = new_df.join(existing_df, join_condition, "left")

    # Set CreatedDate for INSERT records
    if "CreatedDate" in bronze_columns:
        enriched_df = enriched_df.withColumn(
            "CreatedDate",
            when(
                col("Operation") == "I",
                col("LoadDate")
            ).otherwise(col("_target_CreatedDate"))
        )

    # Set ModifiedDate for UPDATE records
    if "ModifiedDate" in bronze_columns:
        if "_target_ModifiedDate" in enriched_df.columns:
            enriched_df = enriched_df.withColumn(
                "ModifiedDate",
                when(
                    col("Operation") == "U",
                    col("LoadDate")
                ).otherwise(col("_target_ModifiedDate"))
            )
        else:
            enriched_df = enriched_df.withColumn(
                "ModifiedDate",
                when(
                    col("Operation") == "U",
                    col("LoadDate")
                ).otherwise(lit(None).cast("timestamp"))
            )

    # Remove helper columns
    helper_columns = [
        c for c in enriched_df.columns
        if c.startswith("_target_") or c == "_Exists"
    ]

    if helper_columns:
        enriched_df = enriched_df.drop(*helper_columns)

    # Keep final Bronze schema
    final_columns = [c for c in bronze_columns if c in enriched_df.columns]
    changed_df = enriched_df.select(*final_columns)

    has_changes = insert_count > 0 or update_count > 0

    if has_changes:

        # Create or append Bronze History
        if not spark.catalog.tableExists(history_table):

            history_df = (
                changed_df
                .withColumn("BatchID", lit(batch_id))
                .withColumn("SourceFile", lit(source_file))
                .withColumn("ProcessedAt", current_timestamp())
            )

            (
                history_df.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(history_table)
            )

            print(f"✅ History table created: {history_table}")

        else:

            history_target = spark.read.table(history_table)
            history_columns = history_target.columns
            history_df = changed_df

            if "BatchID" in history_columns:
                history_df = history_df.withColumn("BatchID", lit(batch_id))

            if "SourceFile" in history_columns:
                history_df = history_df.withColumn("SourceFile", lit(source_file))

            if "ProcessedAt" in history_columns:
                history_df = history_df.withColumn("ProcessedAt", current_timestamp())

            # Validate History schema
            missing_history_columns = [
                c for c in history_columns
                if c not in history_df.columns
            ]

            if missing_history_columns:
                raise Exception(
                    f"{history_table} is missing source columns: "
                    f"{missing_history_columns}"
                )

            history_append_df = history_df.select(*history_columns)

            (
                history_append_df.write
                .format("delta")
                .mode("append")
                .saveAsTable(history_table)
            )

            print(f"✅ History appended: {history_table}")
            print(f"History rows added: {history_append_df.count()}")

    else:
        print(f"ℹ️ No changes for: {history_table}")

    if has_changes:

        # Merge changes into Bronze
        delta_table = DeltaTable.forName(spark, bronze_table)

        merge_condition = " AND ".join(
            [f"target.`{key}` = source.`{key}`" for key in keys]
        )

        update_columns = {
            c: f"source.`{c}`"
            for c in final_columns
            if c not in keys and c != "CreatedDate"
        }

        insert_columns = {
            c: f"source.`{c}`"
            for c in final_columns
        }

        (
            delta_table.alias("target")
            .merge(changed_df.alias("source"), merge_condition)
            .whenMatchedUpdate(set=update_columns)
            .whenNotMatchedInsert(values=insert_columns)
            .execute()
        )

        print("✅ Bronze MERGE completed.")

    else:
        print("ℹ️ No INSERT/UPDATE records.")
        print("MERGE skipped.")

    # Validate final Bronze row count
    rows_after = spark.read.table(bronze_table).count()
    print(f"Bronze rows: {rows_before} → {rows_after}")
    print(f"✅ SUCCESS: {sheet_name}")

# Print final load result
print(f"\n{'=' * 80}")
print("✅ INCREMENTAL LOAD COMPLETED SUCCESSFULLY")
print(f"{'=' * 80}")
print(f"BatchID: {batch_id}")
print(f"SourceFile: {source_file}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
