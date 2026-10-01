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

from datetime import datetime, timedelta

from delta.tables import DeltaTable

from pyspark.sql.functions import (
    col,
    lit
)


# ============================================================
# CONFIGURATION
# ============================================================

RETENTION_DAYS = 30

# True  = Preview فقط بدون حذف
# False = تنفيذ الحذف فعلياً
DRY_RUN = True


# ============================================================
# HISTORY TABLES
# ============================================================

tables = [
    "customers",
    "orders",
    "order_details",
    "products"
]


# ============================================================
# CUTOFF DATE
# ============================================================

cutoff_date = (
    datetime.now()
    - timedelta(days=RETENTION_DAYS)
)


print("=" * 80)
print("BRONZE HISTORY RETENTION")
print("=" * 80)

print(f"Retention Days : {RETENTION_DAYS}")
print(f"Cutoff Date    : {cutoff_date}")
print(f"Dry Run        : {DRY_RUN}")
print("=" * 80)


# ============================================================
# SUMMARY
# ============================================================

summary = []


# ============================================================
# PROCESS HISTORY TABLES
# ============================================================

for table_name in tables:

    history_table = f"bronze_history_{table_name}"

    print("\n" + "=" * 80)
    print(f"PROCESSING: {history_table}")
    print("=" * 80)


    # --------------------------------------------------------
    # 1. TABLE EXISTENCE
    # --------------------------------------------------------

    if not spark.catalog.tableExists(history_table):

        print(
            f"⚠️ Table not found: "
            f"{history_table}"
        )

        summary.append({
            "table": history_table,
            "status": "NOT FOUND",
            "old_rows": 0,
            "remaining_rows": None
        })

        continue


    # --------------------------------------------------------
    # 2. READ HISTORY TABLE
    # --------------------------------------------------------

    df = spark.read.table(
        history_table
    )


    # --------------------------------------------------------
    # 3. CHECK LOADDATE
    # --------------------------------------------------------

    if "LoadDate" not in df.columns:

        print(
            f"❌ LoadDate missing: "
            f"{history_table}"
        )

        summary.append({
            "table": history_table,
            "status": "FAILED - LoadDate missing",
            "old_rows": 0,
            "remaining_rows": None
        })

        continue


    # --------------------------------------------------------
    # 4. TOTAL ROWS BEFORE RETENTION
    # --------------------------------------------------------

    rows_before = df.count()


    # --------------------------------------------------------
    # 5. COUNT OLD RECORDS
    # --------------------------------------------------------

    old_condition = (
        col("LoadDate")
        < lit(cutoff_date)
    )


    old_count = (
        df
        .filter(old_condition)
        .count()
    )


    # --------------------------------------------------------
    # 6. COUNT NULL LOADDATE
    # --------------------------------------------------------

    null_load_date_count = (
        df
        .filter(
            col("LoadDate").isNull()
        )
        .count()
    )


    print(
        f"Rows Before      : "
        f"{rows_before}"
    )

    print(
        f"Old Records      : "
        f"{old_count}"
    )

    print(
        f"NULL LoadDate    : "
        f"{null_load_date_count}"
    )


    # --------------------------------------------------------
    # 7. NULL LOADDATE WARNING
    # --------------------------------------------------------

    if null_load_date_count > 0:

        print(
            f"⚠️ WARNING: "
            f"{null_load_date_count} records "
            f"have NULL LoadDate."
        )

        print(
            "These records will NOT be deleted."
        )


    # --------------------------------------------------------
    # 8. NOTHING TO DELETE
    # --------------------------------------------------------

    if old_count == 0:

        print(
            f"ℹ️ No records older than "
            f"{RETENTION_DAYS} days."
        )

        summary.append({
            "table": history_table,
            "status": "NO DELETIONS",
            "old_rows": 0,
            "remaining_rows": rows_before
        })

        continue


    # --------------------------------------------------------
    # 9. DRY RUN
    # --------------------------------------------------------

    if DRY_RUN:

        print(
            f"🔎 DRY RUN: "
            f"{old_count} records "
            f"would be deleted."
        )

        summary.append({
            "table": history_table,
            "status": "DRY RUN",
            "old_rows": old_count,
            "remaining_rows": rows_before
        })

        continue


    # --------------------------------------------------------
    # 10. ACTUAL DELETE
    # --------------------------------------------------------

    delta_table = DeltaTable.forName(
        spark,
        history_table
    )


    delta_table.delete(
        old_condition
    )


    # --------------------------------------------------------
    # 11. VALIDATE AFTER DELETE
    # --------------------------------------------------------

    remaining_df = spark.read.table(
        history_table
    )


    rows_after = remaining_df.count()


    remaining_old_count = (
        remaining_df
        .filter(old_condition)
        .count()
    )


    deleted_rows = (
        rows_before
        - rows_after
    )


    print(
        f"Rows After       : "
        f"{rows_after}"
    )

    print(
        f"Deleted Rows     : "
        f"{deleted_rows}"
    )

    print(
        f"Old Rows Left    : "
        f"{remaining_old_count}"
    )


    # --------------------------------------------------------
    # 12. VALIDATION
    # --------------------------------------------------------

    if remaining_old_count > 0:

        raise Exception(
            f"Retention validation failed for "
            f"{history_table}. "
            f"{remaining_old_count} old records "
            f"still remain."
        )


    if deleted_rows != old_count:

        raise Exception(
            f"Unexpected delete count for "
            f"{history_table}. "
            f"Expected={old_count}, "
            f"Actual={deleted_rows}"
        )


    print(
        f"✅ Retention completed successfully: "
        f"{history_table}"
    )


    summary.append({
        "table": history_table,
        "status": "DELETED",
        "old_rows": deleted_rows,
        "remaining_rows": rows_after
    })


# ============================================================
# FINAL SUMMARY
# ============================================================

print("\n" + "=" * 80)
print("RETENTION SUMMARY")
print("=" * 80)


for item in summary:

    print(
        f"{item['table']} | "
        f"Status={item['status']} | "
        f"Old Rows={item['old_rows']} | "
        f"Remaining={item['remaining_rows']}"
    )


# ============================================================
# FINAL MESSAGE
# ============================================================

print("\n" + "=" * 80)

if DRY_RUN:

    print(
        "✅ RETENTION PREVIEW COMPLETED"
    )

    print(
        "ℹ️ No records were deleted."
    )

    print(
        "➡️ Change DRY_RUN = False "
        "when you are ready for actual deletion."
    )

else:

    print(
        "✅ RETENTION COMPLETED SUCCESSFULLY"
    )

print("=" * 80)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
