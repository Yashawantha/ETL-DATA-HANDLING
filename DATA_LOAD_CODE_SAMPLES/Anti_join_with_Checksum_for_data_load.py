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


# Table loading – only appending new data
try:
    df_final.createOrReplaceTempView("df_load")

    report_load = spark.sql("""
        SELECT
            s.*
        FROM df_load s
        LEFT ANTI JOIN drvd_table.report t
            ON t.id = s.id
           AND t.report = s.report
           AND t._checksum = s._checksum
    """)

    # Insert inserting new records into the table
    report_load.write.mode("append").insertInto(
        "drvd_table.report"
    )

    write_count_report = report_load.count()
    status = "success"
    message = f"{write_count_report} records inserted in report table"

except Exception as write_count_exception:
    status = "failed"
    message = (
        f"{write_count_exception} "
        "Records not inserted in report table"
    )

print(message)

