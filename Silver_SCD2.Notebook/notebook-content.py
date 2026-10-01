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

from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, coalesce, row_number, lead, current_timestamp
from pyspark.sql.window import Window

# Define SCD2 configuration
scd_config = {
    "customers": {
        "source_table": "silver.customers",
        "target_table": "silver.dim_customers",
        "key_columns": ["CustomerID"]
    },
    "products": {
        "source_table": "silver.products",
        "target_table": "silver.dim_products",
        "key_columns": ["ProductID"]
    }
}

MAX_DATE = "9999-12-31 23:59:59"

# Build business key join condition
def build_join_condition(left_alias, right_alias, key_columns):
    condition = None

    for key in key_columns:
        current_condition = col(f"{left_alias}.{key}") == col(f"{right_alias}.{key}")
        condition = current_condition if condition is None else condition & current_condition

    return condition

# Align DataFrame columns to target data types
def align_to_target_schema(df, target_df):
    target_schema = {field.name: field.dataType for field in target_df.schema.fields}

    for column_name, data_type in target_schema.items():
        if column_name in df.columns:
            df = df.withColumn(column_name, col(column_name).cast(data_type))

    return df

# Repair SCD2 intervals
def repair_scd_intervals(target_table, key_columns):
    df = spark.read.table(target_table)

    duplicate_window = (
        Window
        .partitionBy(*key_columns, "EffectiveFrom")
        .orderBy(col("IsCurrent").desc(), col("LoadDate").desc())
    )

    df = (
        df
        .withColumn("_rn", row_number().over(duplicate_window))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    version_window = (
        Window
        .partitionBy(*key_columns)
        .orderBy(col("EffectiveFrom").asc())
    )

    df = (
        df
        .withColumn("_next_effective_from", lead("EffectiveFrom").over(version_window))
        .withColumn(
            "EffectiveTo",
            coalesce(col("_next_effective_from"), lit(MAX_DATE).cast("timestamp"))
        )
        .withColumn("IsCurrent", col("_next_effective_from").isNull())
        .drop("_next_effective_from")
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

# Process SCD2 dimensions
for table_name, config in scd_config.items():

    source_table = config["source_table"]
    target_table = config["target_table"]
    key_columns = config["key_columns"]

    try:
        # Read source table
        source_df = spark.read.table(source_table)

        # Validate business keys
        for key in key_columns:
            if key not in source_df.columns:
                raise Exception(f"Missing business key: {key}")

        # Ensure timestamp columns
        if "LoadDate" not in source_df.columns:
            source_df = source_df.withColumn("LoadDate", current_timestamp())

        if "CreatedDate" not in source_df.columns:
            source_df = source_df.withColumn("CreatedDate", col("LoadDate"))

        if "ModifiedDate" not in source_df.columns:
            source_df = source_df.withColumn("ModifiedDate", lit(None).cast("timestamp"))

        source_df = (
            source_df
            .withColumn("LoadDate", col("LoadDate").cast("timestamp"))
            .withColumn("CreatedDate", col("CreatedDate").cast("timestamp"))
            .withColumn("ModifiedDate", col("ModifiedDate").cast("timestamp"))
        )

        # Ensure Operation column
        if "Operation" not in source_df.columns:
            source_df = source_df.withColumn("Operation", lit("I"))
        else:
            source_df = source_df.withColumn(
                "Operation",
                coalesce(col("Operation"), lit("I"))
            )

        # Keep latest record per business key
        source_window = (
            Window
            .partitionBy(*key_columns)
            .orderBy(
                coalesce(col("ModifiedDate"), col("LoadDate")).desc(),
                col("LoadDate").desc()
            )
        )

        source_df = (
            source_df
            .withColumn("_rn", row_number().over(source_window))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )

        # Create SCD2 columns
        source_df = (
            source_df
            .withColumn(
                "EffectiveFrom",
                coalesce(col("ModifiedDate"), col("LoadDate")).cast("timestamp")
            )
            .withColumn("EffectiveTo", lit(MAX_DATE).cast("timestamp"))
            .withColumn("IsCurrent", lit(True))
        )

        # Create target table on first run
        if not spark.catalog.tableExists(target_table):
            initial_df = (
                source_df
                .filter(col("Operation") != "DELETE")
                .dropDuplicates(key_columns)
            )

            (
                initial_df.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(target_table)
            )

            print(f"✅ Created: {target_table} | Rows={initial_df.count()}")
            continue

        # Repair existing SCD2 intervals
        repair_scd_intervals(target_table, key_columns)

        # Read target table
        target_df = spark.read.table(target_table)

        # Align source data types
        source_df = align_to_target_schema(source_df, target_df)

        source_df = source_df.select(
            *[col(c) for c in target_df.columns if c in source_df.columns]
        )

        # Get current records
        current_df = target_df.filter(col("IsCurrent") == True)

        # Build join condition
        join_condition = build_join_condition("source", "target", key_columns)

        # Find new records
        new_records_df = (
            source_df
            .filter(col("Operation") != "DELETE")
            .alias("source")
            .join(current_df.alias("target"), join_condition, "left_anti")
        )

        # Find matched records
        matched_df = (
            source_df
            .filter(col("Operation") != "DELETE")
            .alias("source")
            .join(current_df.alias("target"), join_condition, "inner")
        )

        # Define business columns
        excluded_columns = {
            "LoadDate", "CreatedDate", "ModifiedDate",
            "Operation", "EffectiveFrom", "EffectiveTo", "IsCurrent"
        }

        business_columns = [
            c for c in source_df.columns
            if c not in excluded_columns
            and c in target_df.columns
            and c not in key_columns
        ]

        # Build change detection condition
        change_condition = None

        for c in business_columns:
            condition = ~col(f"source.{c}").eqNullSafe(col(f"target.{c}"))
            change_condition = (
                condition if change_condition is None
                else change_condition | condition
            )

        # Process only newer records
        newer_condition = (
            col("source.EffectiveFrom") > col("target.EffectiveFrom")
        )

        # Find changed records
        if change_condition is not None:
            changed_df = (
                matched_df
                .filter(change_condition & newer_condition)
                .select("source.*")
            )
        else:
            changed_df = matched_df.limit(0).select("source.*")

        # Find delete records
        delete_df = (
            source_df
            .filter(col("Operation") == "DELETE")
            .alias("source")
            .join(current_df.alias("target"), join_condition, "inner")
            .filter(newer_condition)
            .select("source.*")
        )

        # Calculate SCD2 changes
        new_count = new_records_df.count()
        changed_count = changed_df.count()
        delete_count = delete_df.count()

        print(
            f"{table_name}: New={new_count} | "
            f"Changed={changed_count} | Deleted={delete_count}"
        )

        # Close old changed records
        if changed_count > 0:
            delta_target = DeltaTable.forName(spark, target_table)

            changed_keys = (
                changed_df
                .select(*key_columns, "EffectiveFrom")
                .dropDuplicates()
            )

            merge_condition = " AND ".join(
                [f"target.`{key}` = source.`{key}`" for key in key_columns]
            )

            (
                delta_target.alias("target")
                .merge(changed_keys.alias("source"), merge_condition)
                .whenMatchedUpdate(
                    condition=(
                        "target.IsCurrent = true "
                        "AND source.EffectiveFrom > target.EffectiveFrom"
                    ),
                    set={
                        "EffectiveTo": "source.EffectiveFrom",
                        "IsCurrent": "false"
                    }
                )
                .execute()
            )

        # Close old deleted records
        if delete_count > 0:
            delta_target = DeltaTable.forName(spark, target_table)

            delete_keys = (
                delete_df
                .select(*key_columns, "EffectiveFrom")
                .dropDuplicates()
            )

            merge_condition = " AND ".join(
                [f"target.`{key}` = source.`{key}`" for key in key_columns]
            )

            (
                delta_target.alias("target")
                .merge(delete_keys.alias("source"), merge_condition)
                .whenMatchedUpdate(
                    condition=(
                        "target.IsCurrent = true "
                        "AND source.EffectiveFrom > target.EffectiveFrom"
                    ),
                    set={
                        "EffectiveTo": "source.EffectiveFrom",
                        "IsCurrent": "false"
                    }
                )
                .execute()
            )

        # Prepare new SCD2 versions
        records_to_insert = new_records_df.unionByName(changed_df)

        # Remove existing SCD2 versions
        if records_to_insert.limit(1).count() > 0:
            existing_versions = (
                target_df
                .select(*key_columns, "EffectiveFrom")
                .dropDuplicates()
            )

            version_condition = build_join_condition("new", "old", key_columns)
            version_condition = (
                version_condition
                & (col("new.EffectiveFrom") == col("old.EffectiveFrom"))
            )

            records_to_insert = (
                records_to_insert
                .alias("new")
                .join(existing_versions.alias("old"), version_condition, "left_anti")
            )

        # Align and insert new versions
        insert_count = records_to_insert.count()

        if insert_count > 0:
            records_to_insert = align_to_target_schema(records_to_insert, target_df)
            records_to_insert = records_to_insert.select(*target_df.columns)

            (
                records_to_insert.write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "false")
                .saveAsTable(target_table)
            )

            print(f"✅ {table_name}: Inserted={insert_count}")

        # Validate final SCD2 structure
        final_df = spark.read.table(target_table)

        invalid_intervals = (
            final_df
            .filter(col("EffectiveFrom") > col("EffectiveTo"))
            .count()
        )

        duplicate_versions = (
            final_df
            .groupBy(*key_columns, "EffectiveFrom")
            .count()
            .filter(col("count") > 1)
            .count()
        )

        multiple_current = (
            final_df
            .filter(col("IsCurrent") == True)
            .groupBy(*key_columns)
            .count()
            .filter(col("count") > 1)
            .count()
        )

        invalid_current = (
            final_df
            .filter(
                col("IsCurrent") != (
                    col("EffectiveTo") == lit(MAX_DATE).cast("timestamp")
                )
            )
            .count()
        )

        # Validate SCD2 integrity
        if invalid_intervals > 0:
            raise Exception(f"{table_name}: {invalid_intervals} invalid intervals.")

        if duplicate_versions > 0:
            raise Exception(f"{table_name}: {duplicate_versions} duplicate versions.")

        if multiple_current > 0:
            raise Exception(f"{table_name}: {multiple_current} multiple current records.")

        if invalid_current > 0:
            raise Exception(f"{table_name}: {invalid_current} invalid IsCurrent flags.")

        # Print final statistics
        total_rows = final_df.count()
        distinct_keys = final_df.select(*key_columns).distinct().count()
        current_rows = final_df.filter(col("IsCurrent") == True).count()
        history_rows = final_df.filter(col("IsCurrent") == False).count()

        print(
            f"✅ {table_name}: Total={total_rows} | "
            f"Keys={distinct_keys} | Current={current_rows} | "
            f"History={history_rows}"
        )

    except Exception as e:
        print(f"❌ SCD2 FAILED: {table_name} | {str(e)}")
        raise

print("✅ SCD TYPE 2 COMPLETED SUCCESSFULLY")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
