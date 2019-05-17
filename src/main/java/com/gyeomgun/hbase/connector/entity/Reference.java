package com.gyeomgun.hbase.connector.entity;

import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;
import java.util.Map;

/**
 * Created by hyungyeom on 19/01/2019.
 */
@Getter
@Setter
public class Reference {
    public static final String REF_COLUMN_FAMILY_NAME = "ref";
    public static final byte[] BYTE_REF_COLUMN_FAMILY_NAME = Bytes.toBytes(REF_COLUMN_FAMILY_NAME);

    private Map<String, List<String>> referenceMap = Maps.newConcurrentMap();
}
