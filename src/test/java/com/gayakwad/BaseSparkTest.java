package com.gayakwad;

import com.holdenkarau.spark.testing.LocalSparkContext$;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.Serializable;

/**
 * Created by thoughtworker on 12/28/15.
 */
public class BaseSparkTest implements Serializable {
    private static transient SparkContext _sc;
    private static transient JavaSparkContext _jsc;
    private static transient SQLContext _ssc;
    protected boolean initialized = false;
    public static SparkConf _conf = new SparkConf().setMaster("local[4]").setAppName("TeStApP");

    public SparkConf conf() {
        return _conf;
    }

    public SparkContext sc() {
        return _sc;
    }

    public JavaSparkContext jsc() {
        return _jsc;
    }

    public SQLContext ssc() {
        return _ssc;
    }

    @Before
    public void runBefore() {
        initialized = (_sc != null);

        if (!initialized) {
            _sc = new SparkContext(conf());
            _jsc = new JavaSparkContext(_sc);
            _ssc = new SQLContext(_sc);
        }
    }

    @AfterClass
    static public void runAfterClass() {
        LocalSparkContext$.MODULE$.stop(_sc);
        _sc = null;
        _jsc = null;
        _ssc = null;
    }
}
