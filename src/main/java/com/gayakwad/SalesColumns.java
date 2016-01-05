package com.gayakwad;

import org.apache.spark.sql.types.*;

/**
 * Created by thoughtworker on 1/4/16.
 */
public enum SalesColumns {
    SUBCLASS(DataTypes.StringType),
    STYLE(DataTypes.StringType),
    SKU(DataTypes.StringType),
    SIZE(DataTypes.StringType);

    private final DataType type;

    public DataType getType() {
        return type;
    }

    SalesColumns(DataType type) {
        this.type = type;
    }

    public static StructType getSchema() {
        final SalesColumns[] values = SalesColumns.values();
        final StructField[] structFields = new StructField[values.length];
        for (int i = 0; i < values.length; i++) {
            structFields[i] = new StructField(values[i].name(), values[i].type, true, Metadata.empty());
        }
        return new StructType(structFields);
    }

    public static StructType getOutputSchema() {
        final StructField[] structFields = {new StructField(SUBCLASS.name(), SUBCLASS.type, true, Metadata.empty()),
                new StructField(STYLE.name(), STYLE.type, true, Metadata.empty()),
                new StructField(SIZE.name(), SIZE.type, true, Metadata.empty())};
        return new StructType(structFields);
    }

}
