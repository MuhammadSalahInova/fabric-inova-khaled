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

from pyspark.sql.functions import col, count, lit
from pyspark.sql.types import (
    StringType,
    LongType,
    DoubleType,
    BooleanType,
    DateType,
    TimestampType
)

# ============================================================
# SILVER FINAL DATA QUALITY CHECK
# ============================================================

tables_config = {

    "customers": {
        "key": ["CustomerID"],
        "required": ["CustomerID", "LoadDate", "CreatedDate", "ModifiedDate"],
        "operation": True
    },

    "orders": {
        "key": ["OrderID"],
        "required": [
            "OrderID",
            "CustomerID",
            "OrderDate",
            "RequiredDate",
            "LoadDate",
            "CreatedDate",
            "ModifiedDate"
        ],
        "operation": True
    },

    "order_details": {
        "key": ["OrderID", "ProductID"],
        "required": [
            "OrderID",
            "ProductID",
            "LoadDate",
            "CreatedDate",
            "ModifiedDate"
        ],
        "operation": True
    },

    "products": {
        "key": ["ProductID"],
        "required": [
            "ProductID",
            "ProductName",
            "LoadDate",
            "CreatedDate",
            "ModifiedDate"
        ],
        "operation": True
    },

    "employees": {
        "key": ["EmployeeID"],
        "required": ["EmployeeID"],
        "operation": False
    },

    "categories": {
        "key": ["CategoryID"],
        "required": ["CategoryID"],
        "operation": False
    },

    "shippers": {
        "key": ["ShipperID"],
        "required": ["ShipperID"],
        "operation": False
    },

    "suppliers": {
        "key": ["SupplierID"],
        "required": ["SupplierID"],
        "operation": False
    },

    "territories": {
        "key": ["TerritoryID"],
        "required": ["TerritoryID"],
        "operation": False
    }
}


# ============================================================
# EXPECTED DATA TYPES
# ============================================================

expected_types = {

    "customers": {
        "CustomerID": StringType(),
        "LoadDate": TimestampType(),
        "CreatedDate": TimestampType(),
        "ModifiedDate": TimestampType()
    },

    "orders": {
        "OrderID": LongType(),
        "CustomerID": StringType(),
        "EmployeeID": LongType(),
        "ShipVia": LongType(),
        "OrderDate": DateType(),
        "RequiredDate": DateType(),
        "ShippedDate": DateType(),
        "Freight": DoubleType(),
        "LoadDate": TimestampType(),
        "CreatedDate": TimestampType(),
        "ModifiedDate": TimestampType()
    },

    "order_details": {
        "OrderID": LongType(),
        "ProductID": LongType(),
        "UnitPrice": DoubleType(),
        "Quantity": LongType(),
        "Discount": DoubleType(),
        "LoadDate": TimestampType(),
        "CreatedDate": TimestampType(),
        "ModifiedDate": TimestampType()
    },

    "products": {
        "ProductID": LongType(),
        "SupplierID": LongType(),
        "CategoryID": LongType(),
        "UnitPrice": DoubleType(),
        "UnitsInStock": LongType(),
        "UnitsOnOrder": LongType(),
        "ReorderLevel": LongType(),
        "Discontinued": BooleanType(),
        "LoadDate": TimestampType(),
        "CreatedDate": TimestampType(),
        "ModifiedDate": TimestampType()
    },

    "employees": {
        "EmployeeID": LongType(),
        "ReportsTo": LongType()
    },

    "categories": {
        "CategoryID": LongType()
    },

    "shippers": {
        "ShipperID": LongType()
    },

    "suppliers": {
        "SupplierID": LongType()
    },

    "territories": {
        "RegionID": LongType()
    }
}


# ============================================================
# FOREIGN KEY CONFIGURATION
# ============================================================

foreign_keys = [

    ("orders", "CustomerID", "customers", "CustomerID"),

    ("orders", "EmployeeID", "employees", "EmployeeID"),

    ("order_details", "OrderID", "orders", "OrderID"),

    ("order_details", "ProductID", "products", "ProductID"),

    ("products", "SupplierID", "suppliers", "SupplierID"),

    ("products", "CategoryID", "categories", "CategoryID"),

    ("employees", "ReportsTo", "employees", "EmployeeID")
]


# ============================================================
# START VALIDATION
# ============================================================

print("=" * 100)
print("SILVER FINAL DATA QUALITY CHECK")
print("=" * 100)

total_issues = 0
warnings = 0


# ============================================================
# TABLE VALIDATION
# ============================================================

for table_name, config in tables_config.items():

    table = f"silver.{table_name}"

    print("\n" + "=" * 100)
    print(f"VALIDATING: {table}")
    print("=" * 100)

    df = spark.read.table(table)

    rows = df.count()
    columns = len(df.columns)

    print(f"Rows    : {rows}")
    print(f"Columns : {columns}")
    print(f"Key     : {config['key']}")

    # --------------------------------------------------------
    # KEY CHECK
    # --------------------------------------------------------

    key_null_condition = None

    for key in config["key"]:

        condition = col(key).isNull()

        if key_null_condition is None:
            key_null_condition = condition
        else:
            key_null_condition = (
                key_null_condition | condition
            )

    key_nulls = (
        df.filter(key_null_condition).count()
    )

    print(f"Key NULLs : {key_nulls}")

    if key_nulls > 0:
        print(f"❌ KEY NULL ISSUE: {key_nulls}")
        total_issues += 1
    else:
        print("✅ Key NULL check: PASS")

    # --------------------------------------------------------
    # DUPLICATE CHECK
    # --------------------------------------------------------

    duplicate_groups = (
        df.groupBy(*config["key"])
        .count()
        .filter(col("count") > 1)
        .count()
    )

    print(f"Duplicate Groups : {duplicate_groups}")

    if duplicate_groups > 0:
        print(
            f"❌ DUPLICATE ISSUE: "
            f"{duplicate_groups}"
        )
        total_issues += 1
    else:
        print("✅ Duplicate check: PASS")

    # --------------------------------------------------------
    # REQUIRED COLUMNS
    # --------------------------------------------------------

    for required_column in config["required"]:

        if required_column not in df.columns:

            print(
                f"❌ Missing Required Column: "
                f"{required_column}"
            )

            total_issues += 1

        else:

            null_count = (
                df.filter(
                    col(required_column).isNull()
                ).count()
            )

            print(
                f"{required_column}: "
                f"NULL={null_count}"
            )

            if null_count > 0:

                print(
                    f"❌ Required NULL: "
                    f"{required_column}"
                )

                total_issues += 1

    # --------------------------------------------------------
    # DATA TYPES
    # --------------------------------------------------------

    if table_name in expected_types:

        actual_schema = {
            field.name: field.dataType
            for field in df.schema.fields
        }

        for column_name, expected_type in (
            expected_types[table_name].items()
        ):

            if column_name not in actual_schema:

                print(
                    f"❌ Missing Type Column: "
                    f"{column_name}"
                )

                total_issues += 1
                continue

            actual_type = actual_schema[column_name]

            if actual_type != expected_type:

                print(
                    f"❌ TYPE ERROR: "
                    f"{column_name} | "
                    f"Expected={expected_type.simpleString()} | "
                    f"Actual={actual_type.simpleString()}"
                )

                total_issues += 1

            else:

                print(
                    f"✅ Type PASS: "
                    f"{column_name} = "
                    f"{actual_type.simpleString()}"
                )

    # --------------------------------------------------------
    # OPERATION CHECK
    # --------------------------------------------------------

    if config["operation"]:

        if "Operation" not in df.columns:

            print(
                "❌ Operation column missing"
            )

            total_issues += 1

        else:

            invalid_operations = (
                df.filter(
                    col("Operation").isNotNull()
                    &
                    ~col("Operation").isin("I", "U", "D")
                )
                .count()
            )

            print(
                f"Invalid Operation values: "
                f"{invalid_operations}"
            )

            if invalid_operations > 0:

                print(
                    "❌ Invalid Operation values"
                )

                total_issues += 1

            else:

                print(
                    "✅ Operation values: PASS"
                )

    else:

        print(
            "ℹ️ Operation column not applicable"
        )


# ============================================================
# FOREIGN KEY VALIDATION
# ============================================================

print("\n" + "=" * 100)
print("FOREIGN KEY VALIDATION")
print("=" * 100)


for (
    child_table,
    child_column,
    parent_table,
    parent_column
) in foreign_keys:

    child = spark.read.table(
        f"silver.{child_table}"
    )

    parent = spark.read.table(
        f"silver.{parent_table}"
    )

    orphan_count = (

        child

        .filter(
            col(child_column).isNotNull()
        )

        .join(

            parent.select(
                col(parent_column)
                .alias("_parent_key")
            ),

            col(child_column)
            ==
            col("_parent_key"),

            "left_anti"
        )

        .count()
    )

    print(
        f"{child_table}.{child_column} "
        f"-> "
        f"{parent_table}.{parent_column} "
        f"| Orphans={orphan_count}"
    )

    if orphan_count > 0:

        print(
            "⚠️ WARNING: Orphan records detected "
            "but validation will continue."
        )

        warnings += 1

    else:

        print("✅ PASS")


# ============================================================
# FINAL SUMMARY
# ============================================================

print("\n" + "=" * 100)
print("SILVER FINAL VALIDATION SUMMARY")
print("=" * 100)

print(
    f"Tables Validated : "
    f"{len(tables_config)}"
)

print(
    f"Critical Issues  : "
    f"{total_issues}"
)

print(
    f"Warnings         : "
    f"{warnings}"
)


# ============================================================
# FINAL RESULT
# ============================================================

if total_issues > 0:

    print("\n" + "!" * 100)

    print(
        f"❌ SILVER VALIDATION FAILED "
        f"WITH {total_issues} CRITICAL ISSUE(S)"
    )

    print(
        "Gold layer should NOT start."
    )

    print("!" * 100)

    raise Exception(
        f"Silver Validation Failed "
        f"with {total_issues} critical issue(s)."
    )

else:

    print("\n" + "=" * 100)

    print(
        "✅ ALL SILVER VALIDATIONS PASSED SUCCESSFULLY"
    )

    if warnings > 0:

        print(
            f"⚠️ {warnings} warning(s) detected."
        )

        print(
            "These warnings do not stop the pipeline."
        )

    print(
        "🚀 SILVER IS READY FOR GOLD"
    )

    print("=" * 100)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
