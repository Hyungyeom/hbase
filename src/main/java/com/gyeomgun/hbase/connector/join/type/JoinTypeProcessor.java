package com.gyeomgun.hbase.connector.join.type;


import com.gyeomgun.hbase.connector.entity.HbaseEntity;
import com.gyeomgun.hbase.connector.join.Row;

import java.util.List;
import java.util.Set;

public interface JoinTypeProcessor {
    JoinByType getJoinByType();

    <T extends HbaseEntity> Set<byte[]> extractFromKey(JoinTypeProcessorParameter parameter,
                                                       List<HbaseEntity> fromData);

    <T extends HbaseEntity> void mergeByRowkey(JoinTypeProcessorParameter parameter, List<Row> rows,
                                               Set<byte[]> fromKeys);
}
