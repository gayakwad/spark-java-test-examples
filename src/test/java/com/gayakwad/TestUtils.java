package com.gayakwad;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Created by thoughtworker on 1/4/16.
 */
public class TestUtils {

    DataFrame salesDataFrame(SQLContext sqlContext) {
        final DataFrame df = sqlContext.read()
                .format("com.databricks.spark.csv")
                .schema(SalesColumns.getSchema())
                .option("header", "true")
                .load("test-data/sales-txn.csv");
        df.printSchema();
        return df;
    }

}
