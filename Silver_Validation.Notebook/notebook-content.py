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

# MARKDOWN ********************

# ********
#  فحص شامل على كل جداول Silver

# CELL ********************

from pyspark.sql.functions import col

# Define Silver validation configuration
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
        "not_null_cols": ["EmployeeID"]
    },
    "categories": {
        "key_columns": ["CategoryID"],
        "string_cols": ["CategoryName", "Description"],
        "date_cols": [],
        "timestamp_cols": [],
        "numeric_cols": {"CategoryID": "long"},
        "boolean_cols": {},
        "not_null_cols": ["CategoryID"]
    },
    "shippers": {
        "key_columns": ["ShipperID"],
        "string_cols": ["CompanyName", "Phone"],
        "date_cols": [],
        "timestamp_cols": [],
        "numeric_cols": {"ShipperID": "long"},
        "boolean_cols": {},
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
        "not_null_cols": ["SupplierID"]
    },
    "territories": {
        "key_columns": ["TerritoryID"],
        "string_cols": ["TerritoryID", "TerritoryDescription"],
        "date_cols": [],
        "timestamp_cols": [],
        "numeric_cols": {"RegionID": "long"},
        "boolean_cols": {},
        "not_null_cols": ["TerritoryID"]
    }
}

# Define numeric business rules
tables_numeric_checks = {
    "orders": ["Freight"],
    "order_details": ["UnitPrice", "Quantity", "Discount"],
    "products": ["UnitPrice", "UnitsInStock", "UnitsOnOrder", "ReorderLevel"]
}

# Define foreign key relationships
foreign_keys = [
    {
        "table": "orders", "fk_col": "CustomerID",
        "ref_table": "customers", "ref_col": "CustomerID", "nullable": False
    },
    {
        "table": "orders", "fk_col": "EmployeeID",
        "ref_table": "employees", "ref_col": "EmployeeID", "nullable": True
    },
    {
        "table": "order_details", "fk_col": "OrderID",
        "ref_table": "orders", "ref_col": "OrderID", "nullable": False
    },
    {
        "table": "order_details", "fk_col": "ProductID",
        "ref_table": "products", "ref_col": "ProductID", "nullable": False
    },
    {
        "table": "products", "fk_col": "SupplierID",
        "ref_table": "suppliers", "ref_col": "SupplierID", "nullable": True
    },
    {
        "table": "products", "fk_col": "CategoryID",
        "ref_table": "categories", "ref_col": "CategoryID", "nullable": True
    },
    {
        "table": "employees", "fk_col": "ReportsTo",
        "ref_table": "employees", "ref_col": "EmployeeID", "nullable": True
    }
]

validation_errors = []

# Normalize Spark data types
def get_actual_type(df, column_name):
    if column_name not in df.columns:
        return None

    actual_type = df.schema[column_name].dataType.simpleString()
    return "long" if actual_type == "bigint" else actual_type

# Validate column existence, data type, and NULL values
def check_columns(df, columns, table_name, category):
    if isinstance(columns, dict):
        columns_to_check = columns.items()
    else:
        expected_type_map = {
            "STRING": "string",
            "DATE": "date",
            "TIMESTAMP": "timestamp",
            "BOOLEAN": "boolean"
        }
        columns_to_check = [(c, expected_type_map[category]) for c in columns]

    for column_name, expected_type in columns_to_check:
        if column_name not in df.columns:
            validation_errors.append(f"{table_name}: {column_name} missing")
            print(f"❌ {column_name}: MISSING")
            continue

        actual_type = get_actual_type(df, column_name)
        type_pass = actual_type == expected_type
        null_count = df.filter(col(column_name).isNull()).count()

        print(
            f"{column_name}: Type={'PASS' if type_pass else 'FAIL'} "
            f"| Expected={expected_type} | Actual={actual_type} | NULL={null_count}"
        )

        if not type_pass:
            validation_errors.append(
                f"{table_name}: {column_name} datatype mismatch "
                f"(Expected={expected_type}, Actual={actual_type})"
            )

# Validate Silver table structure and data quality
def validate_table(table_name, config):
    full_table = f"silver.{table_name}"

    print(f"\n{'=' * 80}\nVALIDATING: {full_table}\n{'=' * 80}")

    # Read Silver table
    try:
        df = spark.read.table(full_table)
    except Exception as e:
        validation_errors.append(f"{full_table}: TABLE READ FAILED - {e}")
        print(f"❌ TABLE READ FAILED: {e}")
        return

    total_rows = df.count()
    print(f"Rows={total_rows} | Columns={len(df.columns)}")

    # Validate business keys
    key_columns = config["key_columns"]
    missing_keys = [c for c in key_columns if c not in df.columns]

    if missing_keys:
        validation_errors.append(
            f"{full_table}: KEY COLUMNS MISSING = {missing_keys}"
        )
        print(f"❌ Missing key columns: {missing_keys}")
    else:
        null_condition = None

        for c in key_columns:
            condition = col(c).isNull()
            null_condition = condition if null_condition is None else null_condition | condition

        null_key_count = df.filter(null_condition).count()
        valid_key_df = df.filter(~null_condition)
        distinct_key_count = valid_key_df.select(*key_columns).distinct().count()
        valid_key_count = valid_key_df.count()
        duplicate_count = valid_key_count - distinct_key_count

        print(
            f"KEY={key_columns} | NULL={null_key_count} | "
            f"DUPLICATES={duplicate_count}"
        )

        if null_key_count > 0:
            validation_errors.append(
                f"{full_table}: {null_key_count} NULL BUSINESS KEYS"
            )

        if duplicate_count > 0:
            validation_errors.append(
                f"{full_table}: {duplicate_count} DUPLICATE BUSINESS KEYS"
            )

    # Validate string columns
    check_columns(df, config["string_cols"], full_table, "STRING")

    # Validate date columns
    check_columns(df, config["date_cols"], full_table, "DATE")

    # Validate timestamp columns
    if config["timestamp_cols"]:
        check_columns(df, config["timestamp_cols"], full_table, "TIMESTAMP")
    else:
        print("✅ No timestamp columns required")

    # Validate numeric columns
    check_columns(df, config["numeric_cols"], full_table, "NUMERIC")

    # Validate boolean columns
    check_columns(df, config["boolean_cols"], full_table, "BOOLEAN")

    # Validate required columns
    for c in config.get("not_null_cols", []):
        if c not in df.columns:
            validation_errors.append(
                f"{full_table}: {c} missing for NOT NULL validation"
            )
            continue

        null_count = df.filter(col(c).isNull()).count()
        print(f"{c}: Required NULL={null_count}")

        if null_count > 0:
            validation_errors.append(
                f"{full_table}: {c} has {null_count} NULL values"
            )

    # Validate timestamp quality
    for timestamp_col in ["LoadDate", "CreatedDate", "ModifiedDate"]:
        if timestamp_col not in df.columns:
            continue

        null_count = df.filter(col(timestamp_col).isNull()).count()
        print(f"{timestamp_col}: NULL={null_count}")

        if null_count > 0:
            validation_errors.append(
                f"{full_table}: {timestamp_col} has {null_count} NULL values"
            )

    # Validate date logic
    if "OrderDate" in df.columns and "RequiredDate" in df.columns:
        invalid_dates = df.filter(
            col("OrderDate") > col("RequiredDate")
        ).count()

        print(f"OrderDate > RequiredDate: {invalid_dates}")

        if invalid_dates > 0:
            validation_errors.append(
                f"{full_table}: {invalid_dates} records where "
                f"OrderDate > RequiredDate"
            )

    if "OrderDate" in df.columns and "ShippedDate" in df.columns:
        invalid_dates = df.filter(
            col("ShippedDate") < col("OrderDate")
        ).count()

        print(f"ShippedDate < OrderDate: {invalid_dates}")

        if invalid_dates > 0:
            validation_errors.append(
                f"{full_table}: {invalid_dates} records where "
                f"ShippedDate < OrderDate"
            )

    # Validate numeric business rules
    for c in tables_numeric_checks.get(table_name, []):
        if c not in df.columns:
            continue

        negative_count = df.filter(col(c) < 0).count()
        print(f"{c}: Negative={negative_count}")

        if negative_count > 0:
            validation_errors.append(
                f"{full_table}: {c} has {negative_count} negative values"
            )

    # Validate discount range
    if table_name == "order_details" and "Discount" in df.columns:
        invalid_discount = df.filter(
            (col("Discount") < 0) | (col("Discount") > 1)
        ).count()

        print(f"Discount outside [0,1]: {invalid_discount}")

        if invalid_discount > 0:
            validation_errors.append(
                f"{full_table}: {invalid_discount} invalid Discount values"
            )

    # Validate Operation values
    if "Operation" in df.columns:
        invalid_operations = df.filter(
            col("Operation").isNotNull()
            & ~col("Operation").isin("I", "U")
        ).count()

        print(f"Invalid Operation values: {invalid_operations}")

        if invalid_operations > 0:
            validation_errors.append(
                f"{full_table}: {invalid_operations} invalid "
                f"Operation values (Allowed: I, U, NULL)"
            )

        operation_counts = (
            df.groupBy("Operation")
            .count()
            .orderBy("Operation")
            .collect()
        )

        print("Operation distribution:")

        for row in operation_counts:
            print(f"  {row['Operation']} : {row['count']}")
    else:
        print("✅ Operation column not applicable")

# Validate foreign key relationships
def validate_fk(fk):
    table = f"silver.{fk['table']}"
    ref_table = f"silver.{fk['ref_table']}"
    fk_col = fk["fk_col"]
    ref_col = fk["ref_col"]

    try:
        df = spark.read.table(table)
        ref_df = spark.read.table(ref_table)
    except Exception as e:
        validation_errors.append(f"FK READ FAILED: {e}")
        return

    # Validate FK column existence
    if fk_col not in df.columns:
        validation_errors.append(f"{table}: FK column {fk_col} missing")
        return

    if ref_col not in ref_df.columns:
        validation_errors.append(
            f"{ref_table}: Referenced column {ref_col} missing"
        )
        return

    # Validate FK data types
    fk_type = df.schema[fk_col].dataType.simpleString()
    ref_type = ref_df.schema[ref_col].dataType.simpleString()

    print(
        f"{table}.{fk_col} type={fk_type} | "
        f"{ref_table}.{ref_col} type={ref_type}"
    )

    if fk_type != ref_type:
        validation_errors.append(
            f"{table}.{fk_col} -> {ref_table}.{ref_col}: "
            f"DATATYPE MISMATCH ({fk_type} vs {ref_type})"
        )
        return

    # Ignore nullable FK values
    if fk.get("nullable", False):
        df = df.filter(col(fk_col).isNotNull())

    # Check orphan records
    orphan_df = (
        df.join(
            ref_df.select(col(ref_col).alias("_ref_key")).dropDuplicates(),
            col(fk_col) == col("_ref_key"),
            "left_anti"
        )
    )

    orphan_count = orphan_df.count()
    label = f"{table}.{fk_col} -> {ref_table}.{ref_col}"

    # Orphans are warnings and do not stop the pipeline
    if orphan_count == 0:
        print(f"{label}: PASS | Orphans=0")
    else:
        print(f"⚠️ {label}: WARNING | Orphans={orphan_count}")
        print("   Orphan records detected but validation will continue.")

# Run table validations
for table_name, config in table_config.items():
    validate_table(table_name, config)

# Run foreign key validations
print(f"\n{'=' * 80}\nFOREIGN KEY VALIDATION\n{'=' * 80}")

for fk in foreign_keys:
    validate_fk(fk)

# Print final validation result
print(f"\n{'=' * 80}\nSILVER VALIDATION SUMMARY\n{'=' * 80}")

if validation_errors:
    print(f"❌ TOTAL ISSUES: {len(validation_errors)}")

    for error in validation_errors:
        print(f"❌ {error}")

    raise Exception(
        f"Silver Validation Failed with {len(validation_errors)} issue(s)."
    )
else:
    print("✅ ALL SILVER VALIDATIONS PASSED SUCCESSFULLY.")
    print(f"Validated tables: {len(table_config)}")
    print(
        "⚠️ Note: Foreign-key orphan records are treated as warnings "
        "and do not stop the pipeline."
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
