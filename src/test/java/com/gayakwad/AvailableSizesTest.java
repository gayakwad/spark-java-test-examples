package com.gayakwad;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by thoughtworker on 1/4/16.
 */
public class AvailableSizesTest extends BaseSparkTest {

    @Test
    public void shouldCreateSizeList() {
        final SQLContext ssc = ssc();
        final DataFrame dataFrame = new TestUtils().salesDataFrame(ssc);
        final DataFrame sizeDF = new AvailableSizes().calculate(ssc, dataFrame);
        assertEquals(sizeDF.count(), 3);
    }

}