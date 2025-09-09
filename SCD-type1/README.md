# SCD Type 1 in Azure Databricks

This document explains how to implement Slowly Changing Dimension (SCD) Type 1 in Azure Databricks.

## What is SCD Type 1?

SCD Type 1 overwrites old data with new data in a dimension table. It does not keep historical changes.

## Steps to Implement SCD Type 1

1. **Read Source Data**
    ```python
    source_df = spark.read.format("csv").option("header", "true").load("path/to/source.csv")
    ```

2. **Read Target Dimension Table**
    ```python
    target_df = spark.read.format("delta").load("path/to/dimension_table")
    ```

3. **Identify Records to Update**
    ```python
    from pyspark.sql.functions import col

    updates_df = source_df.join(
         target_df,
         on="business_key",
         how="inner"
    ).filter(
         source_df["attribute"] != target_df["attribute"]
    )
    ```

4. **Overwrite Records in Target Table**
    ```python
    from delta.tables import DeltaTable

    delta_table = DeltaTable.forPath(spark, "path/to/dimension_table")
    delta_table.alias("tgt").merge(
         source_df.alias("src"),
         "tgt.business_key = src.business_key"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    ```

## Best Practices

- Use Delta Lake for ACID transactions.
- Schedule regular updates using Databricks Jobs.
- Monitor data quality and consistency.

## References

- [Azure Databricks Documentation](https://docs.microsoft.com/en-us/azure/databricks/)
- [Delta Lake Merge](https://docs.delta.io/latest/delta-update.html)