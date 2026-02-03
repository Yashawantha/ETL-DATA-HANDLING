# removing the duplicates with loading the logic into the derived table.

# Variable Initialization
ignore_column_list_md5 = [
    '_checksum',
    '_az_insert_ts',
    '_az_update_ts',
    '_exec_run_id'
]


def concat_checksum_cols(df):
    bizColList = [
        col for col in df.columns
        if col not in ignore_column_list_md5
    ]

    columnList = []
    for column in bizColList:
        if column is None:
            columnList.append(':')
        else:
            columnList.append(column)

    print(columnList)
    return columnList


# Implementing the checksum to the final logic table
df_final = (
    df.withColumn(
        "REPORT_MONTH",
        date_format(col("date"), "yyyy-MM")
    )
    .withColumn(
        "_az_insert_ts",
        current_timestamp()
    )
    .withColumn(
        "_az_update_ts",
        current_timestamp()
    )
    .withColumn(
        "_exec_run_id",
        lit(exeRunID)
    )
    .withColumn(
        "_checksum",
        md5(concat_ws("|", *concat_checksum_cols(df)))
    )
)
