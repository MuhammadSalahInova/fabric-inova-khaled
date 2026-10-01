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

from pyspark.sql.functions import col


# Define Bronze validation rules
tables_config = {
    "bronze_customers": {
        "key_columns": ["CustomerID"],
        "required_columns": ["CustomerID"],
        "metadata_columns": ["LoadDate", "CreatedDate", "ModifiedDate", "Operation"]
    },
    "bronze_orders": {
        "key_columns": ["OrderID"],
        "required_columns": ["OrderID", "CustomerID"],
        "metadata_columns": ["LoadDate", "CreatedDate", "ModifiedDate", "Operation"]
    },
    "bronze_order_details": {
        "key_columns": ["OrderID", "ProductID"],
        "required_columns": ["OrderID", "ProductID"],
        "metadata_columns": ["LoadDate", "CreatedDate", "ModifiedDate", "Operation"]
    },
    "bronze_products": {
        "key_columns": ["ProductID"],
        "required_columns": ["ProductID", "ProductName"],
        "metadata_columns": ["LoadDate", "CreatedDate", "ModifiedDate", "Operation"]
    },
    "bronze_employees": {
        "key_columns": ["EmployeeID"],
        "required_columns": ["EmployeeID"],
        "metadata_columns": []
    },
    "bronze_categories": {
        "key_columns": ["CategoryID"],
        "required_columns": ["CategoryID"],
        "metadata_columns": []
    },
    "bronze_shippers": {
        "key_columns": ["ShipperID"],
        "required_columns": ["ShipperID"],
        "metadata_columns": []
    },
    "bronze_suppliers": {
        "key_columns": ["SupplierID"],
        "required_columns": ["SupplierID"],
        "metadata_columns": []
    },
    "bronze_territories": {
        "key_columns": ["TerritoryID"],
        "required_columns": ["TerritoryID"],
        "metadata_columns": []
    }
}

# Define expected data types
expected_types = {
    "bronze_orders": {
        "OrderID": "bigint",
        "EmployeeID": "bigint",
        "ShipVia": "bigint",
        "Freight": "double"
    },
    "bronze_order_details": {
        "OrderID": "bigint",
        "ProductID": "bigint",
        "UnitPrice": "double",
        "Quantity": "bigint",
        "Discount": "double"
    },
    "bronze_products": {
        "ProductID": "bigint",
        "SupplierID": "bigint",
        "CategoryID": "bigint",
        "UnitPrice": "double",
        "UnitsInStock": "bigint",
        "UnitsOnOrder": "bigint",
        "ReorderLevel": "bigint"
    }
}

# Define numeric business rules
numeric_rules = {
    "bronze_orders": {
        "Freight": {"min": 0}
    },
    "bronze_order_details": {
        "UnitPrice": {"min": 0},
        "Quantity": {"min": 1},
        "Discount": {"min": 0, "max": 1}
    },
    "bronze_products": {
        "UnitPrice": {"min": 0},
        "UnitsInStock": {"min": 0},
        "UnitsOnOrder": {"min": 0},
        "ReorderLevel": {"min": 0}
    }
}

validation_errors = []

# Validate Bronze tables
for table_name, config in tables_config.items():

    print(f"\n{'=' * 80}\nTABLE: {table_name}\n{'=' * 80}")

    # Read Bronze table
    try:
        df = spark.read.table(table_name)
    except Exception as e:
        validation_errors.append(f"{table_name}: TABLE READ FAILED - {e}")
        print(f"❌ Cannot read table: {e}")
        continue

    rows_count = df.count()
    print(f"Rows    : {rows_count}")
    print(f"Columns : {len(df.columns)}")

    # Validate required columns
    missing_columns = [
        c for c in config["required_columns"]
        if c not in df.columns
    ]

    if missing_columns:
        validation_errors.append(
            f"{table_name}: MISSING REQUIRED COLUMNS = {missing_columns}"
        )
        print(f"❌ Missing columns: {missing_columns}")
    else:
        print("✅ Required columns exist")

    # Validate metadata columns
    for metadata_column in config.get("metadata_columns", []):

        if metadata_column not in df.columns:
            validation_errors.append(
                f"{table_name}: MISSING METADATA COLUMN = {metadata_column}"
            )
            print(f"❌ Missing metadata: {metadata_column}")
            continue

        actual_type = df.schema[metadata_column].dataType.simpleString()
        print(f"{metadata_column}: Type={actual_type}")

        if metadata_column in ["LoadDate", "CreatedDate", "ModifiedDate"]:

            if actual_type != "timestamp":
                validation_errors.append(
                    f"{table_name}: {metadata_column} must be timestamp, got {actual_type}"
                )

            null_count = df.filter(col(metadata_column).isNull()).count()
            print(f"{metadata_column} NULL: {null_count}")

            if null_count > 0:
                validation_errors.append(
                    f"{table_name}: {metadata_column} has {null_count} NULL values"
                )

    # Validate business keys
    key_columns = config["key_columns"]
    missing_keys = [c for c in key_columns if c not in df.columns]

    if missing_keys:
        validation_errors.append(
            f"{table_name}: KEY COLUMN(S) MISSING = {missing_keys}"
        )
        print(f"❌ Missing key columns: {missing_keys}")

    else:
        null_condition = None

        for c in key_columns:
            condition = col(c).isNull()
            null_condition = (
                condition
                if null_condition is None
                else null_condition | condition
            )

        null_key_count = df.filter(null_condition).count()

        print(f"Key columns : {key_columns}")
        print(f"NULL keys   : {null_key_count}")

        if null_key_count > 0:
            validation_errors.append(
                f"{table_name}: {null_key_count} NULL BUSINESS KEYS"
            )

        # Validate duplicate business keys
        valid_key_df = df.filter(~null_condition)
        total_key_rows = valid_key_df.count()
        distinct_key_rows = valid_key_df.select(*key_columns).dropDuplicates().count()
        duplicate_count = total_key_rows - distinct_key_rows

        print(f"Duplicate keys : {duplicate_count}")

        if duplicate_count > 0:
            validation_errors.append(
                f"{table_name}: {duplicate_count} DUPLICATE BUSINESS KEYS"
            )

    # Validate OData metadata
    metadata_found = [
        c for c in df.columns
        if c.startswith("@odata.") or c.endswith(".@odata.etag")
    ]

    if metadata_found:
        validation_errors.append(
            f"{table_name}: ODATA METADATA FOUND = {metadata_found}"
        )
        print(f"❌ OData metadata found: {metadata_found}")
    else:
        print("✅ No OData metadata")

    # Validate value. prefix
    value_prefix_columns = [
        c for c in df.columns
        if c.startswith("value.")
    ]

    if value_prefix_columns:
        validation_errors.append(
            f"{table_name}: 'value.' PREFIX FOUND = {value_prefix_columns}"
        )
        print(f"❌ value. prefix found: {value_prefix_columns}")
    else:
        print("✅ No value. prefix")

    # Validate data types
    for column_name, expected_type in expected_types.get(table_name, {}).items():

        if column_name not in df.columns:
            validation_errors.append(
                f"{table_name}: {column_name} missing for datatype validation"
            )
            continue

        actual_type = df.schema[column_name].dataType.simpleString()

        print(
            f"{column_name}: Expected={expected_type} | "
            f"Actual={actual_type}"
        )

        if actual_type != expected_type:
            validation_errors.append(
                f"{table_name}: {column_name} datatype mismatch "
                f"(Expected={expected_type}, Actual={actual_type})"
            )

    # Validate numeric business rules
    for column_name, rules in numeric_rules.get(table_name, {}).items():

        if column_name not in df.columns:
            continue

        if "min" in rules:
            invalid_count = df.filter(col(column_name) < rules["min"]).count()
            print(f"{column_name}: Below Min={invalid_count}")

            if invalid_count > 0:
                validation_errors.append(
                    f"{table_name}: {column_name} has "
                    f"{invalid_count} values below minimum"
                )

        if "max" in rules:
            invalid_count = df.filter(col(column_name) > rules["max"]).count()
            print(f"{column_name}: Above Max={invalid_count}")

            if invalid_count > 0:
                validation_errors.append(
                    f"{table_name}: {column_name} has "
                    f"{invalid_count} values above maximum"
                )

# Print final validation status
print(f"\n{'=' * 80}\nBRONZE VALIDATION SUMMARY\n{'=' * 80}")

if validation_errors:
    print(f"❌ TOTAL ISSUES FOUND: {len(validation_errors)}")

    for error in validation_errors:
        print(f"❌ {error}")

    raise Exception(
        f"Bronze Validation Failed with {len(validation_errors)} issue(s)."
    )

else:
    print("✅ ALL BRONZE VALIDATIONS PASSED SUCCESSFULLY.")
    print(f"Validated tables: {len(tables_config)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
