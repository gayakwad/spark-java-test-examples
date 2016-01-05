package com.gayakwad;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Created by thoughtworker on 1/4/16.
 */
public class AvailableSizes implements Serializable {

    public DataFrame calculate(SQLContext ssc, DataFrame salesDataFrame) {
        final JavaRDD<Row> rowJavaRDD = salesDataFrame.toJavaRDD();
        JavaPairRDD<String, Row> pairs = rowJavaRDD.mapToPair(
                (PairFunction<Row, String, Row>) row -> {
                    final Object[] objects = {row.getAs(0), row.getAs(1), row.getAs(3)};
                    return new Tuple2<>(row.getAs(SalesColumns.STYLE.name()), new GenericRowWithSchema(objects, SalesColumns.getOutputSchema()));
                });

        JavaPairRDD<String, Row> withSizeList = pairs.reduceByKey(new Function2<Row, Row, Row>() {
            @Override
            public Row call(Row aRow, Row bRow) {
                final String uniqueCommaSeparatedSizes = uniqueSizes(aRow, bRow);
                final Object[] objects = {aRow.getAs(0), aRow.getAs(1), uniqueCommaSeparatedSizes};
                return new GenericRowWithSchema(objects, SalesColumns.getOutputSchema());
            }

            private String uniqueSizes(Row aRow, Row bRow) {
                final TreeSet<String> allSizes = new TreeSet<>();
                final List<String> aSizes = Arrays.asList(((String) aRow.getAs(String.valueOf(SalesColumns.SIZE))).split(","));
                final List<String> bSizes = Arrays.asList(((String) bRow.getAs(String.valueOf(SalesColumns.SIZE))).split(","));
                allSizes.addAll(aSizes);
                allSizes.addAll(bSizes);
                return csvFormat(allSizes);
            }
        });

        final JavaRDD<Row> values = withSizeList.values();
        return ssc.createDataFrame(values, SalesColumns.getOutputSchema());

    }

    public String csvFormat(Collection<String> collection) {
        return collection.stream().map(Object::toString).collect(Collectors.joining(","));
    }
}
