package com.gyeomgun.hbase.connector.filter;


import com.gyeomgun.hbase.connector.entity.HbaseEntity;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class FilterHelper {
    public static SingleColumnValueFilter getQualifierFilter(String qualifier, CompareOperator compareOp,
                                                             byte[] value) {
        return new SingleColumnValueFilter(HbaseEntity.BYTE_DATA_COLUMN_FAMILY_NAME, Bytes.toBytes(qualifier),
                compareOp, value);
    }
}
