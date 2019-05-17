package com.gyeomgun.hbase.connector.join;


import com.gyeomgun.hbase.connector.context.Context;
import com.gyeomgun.hbase.connector.entity.HbaseEntity;
import com.gyeomgun.hbase.connector.join.type.JoinByType;

import java.lang.reflect.Field;
import java.util.List;
import java.util.function.BiPredicate;

public interface JoinNode<T extends HbaseEntity> {
    boolean hasNext();
    JoinNode next();
    void setNext(JoinNode joinNode);
    void setContext(Context<T> context);
    void setKeys(String fromAlias, String fromKey, String joinAlias, String joinKey);
    void setJoinByType(JoinByType joinByType);
    void setReferenceField(Field referenceField);
    List<Row> execute(List<Row> rows);
    void addOnConditions(BiPredicate<T, T> check);
    String getJoinAlias();
    Context<T> getContext();
}
