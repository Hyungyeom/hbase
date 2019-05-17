package com.gyeomgun.hbase.connector.entity;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by hyungyeom on 19/01/2019.
 */
public interface HbaseEntity {
    String DATA_COLUMN_FAMILY_NAME = "data";
    String INDEX_COLUMN_FAMILY_NAME = "index";
    byte[] BYTE_DATA_COLUMN_FAMILY_NAME = Bytes.toBytes(DATA_COLUMN_FAMILY_NAME);
    byte[] BYTE_INDEX_COLUMN_FAMILY_NAME = Bytes.toBytes(INDEX_COLUMN_FAMILY_NAME);

    String getSalt();
    String getRowkey();
    String getRowkey(Object obj);

}
