import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/**
 * Created by thoughtworker on 12/21/15.
 */

public class SparkContextTest {
    @Test
    public void shouldTestCreationOfSparkConf() {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("Test App");
        JavaSparkContext sc = new JavaSparkContext(conf);

        final JavaRDD<Object> objectJavaRDD = sc.emptyRDD();
        assertNotNull(objectJavaRDD);
    }
}


