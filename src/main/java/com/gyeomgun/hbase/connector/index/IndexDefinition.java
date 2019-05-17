package com.gyeomgun.hbase.connector.index;

import com.gyeomgun.hbase.connector.entity.HbaseEntity;

import java.util.Collection;

public interface IndexDefinition<T extends HbaseEntity> {
    Collection<byte[]> getFamilies();
    Collection<byte[]> getTargetFamilies(T t);
}
