package com.gyeomgun.hbase.connector.join.type;


import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.gyeomgun.hbase.connector.Utils;
import com.gyeomgun.hbase.connector.annotation.ColumnQualifier;
import com.gyeomgun.hbase.connector.annotation.Index;
import com.gyeomgun.hbase.connector.context.Context;
import com.gyeomgun.hbase.connector.entity.HbaseEntity;
import com.gyeomgun.hbase.connector.join.Row;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import static com.gyeomgun.hbase.connector.index.IndexManager.BYTE_CF;
import static com.gyeomgun.hbase.connector.index.IndexManager.BYTE_CQ;


public class IndexJoinTypeProcessor implements JoinTypeProcessor {

    @Override
    public JoinByType getJoinByType() {
        return JoinByType.INDEX;
    }

    @Override
    public <T extends HbaseEntity> Set<byte[]> extractFromKey(JoinTypeProcessorParameter parameter,
                                                              List<HbaseEntity> fromData) {

        String fromKey = parameter.getFromKey();
        Context<T> joinContext = parameter.getJoinContext();
        String joinKey = parameter.getJoinKey();

        HbaseEntity fromSample = fromData.stream().findFirst().get();
        Optional<Field> optFromField = Arrays.stream(fromSample.getClass().getDeclaredFields())
                                             .filter(s -> s.getAnnotation(ColumnQualifier.class) != null)
                                             .filter(s -> s.getAnnotation(ColumnQualifier.class).name()
                                                           .equals(fromKey))
                                             .findFirst();
        Preconditions.checkState(optFromField.isPresent(), "Could not found from field. " + fromKey);
        Field fromField = optFromField.get();
        fromField.setAccessible(true);
        parameter.setFromField(fromField);
        Optional<Field> optJoinField = Arrays.stream(joinContext.getMClass().getDeclaredFields())
                                             .filter(s -> s.getAnnotation(Index.class) != null)
                                             .filter(s -> s.getAnnotation(ColumnQualifier.class).name()
                                                           .equals(joinKey))
                                             .findFirst();
        Preconditions.checkState(optJoinField.isPresent(), "Could not found join field. " + joinKey);
        Field joinField = optJoinField.get();
        joinField.setAccessible(true);
        parameter.setJoinField(joinField);

        List<byte[]> keys = fromData.stream()
                                    .map(s -> {
                                        Object key = null;
                                        try {
                                            key = fromField.get(s);
                                        } catch (IllegalAccessException e) {
                                            e.printStackTrace();
                                            return null;
                                        }
                                        if (key == null) {
                                            return null;
                                        }
                                        return key;
                                    })
                                    .collect(Collectors.toSet()).stream()
                                    .map(s -> Utils.getBytes(s))
                                    .collect(Collectors.toList());

        String idxName = parameter.getIndexManager()
                                  .createIndexTableName(joinContext.getTableName(), joinKey);

        ResultScanner scanner = parameter.getIndexManager().getScanner(idxName, keys);
        Set<byte[]> results = Sets.newHashSet();
        while (true) {
            try {
                Result z = scanner.next();
                if (z == null || z.isEmpty()) {
                    break;
                }
                results.add(z.getValue(BYTE_CF, BYTE_CQ));
            } catch (IOException e) {
                e.printStackTrace();
                break;
            }
        }
        return results;

    }

    @Override
    public <T extends HbaseEntity> void mergeByRowkey(JoinTypeProcessorParameter parameter, List<Row> rows,
                                                      Set<byte[]> fromKeys) {
        try {
            Context<T> joinContext = parameter.getJoinContext();
            String fromKey = parameter.getFromKey();
            List<T> joinableData = joinContext.getRepository().findBy(fromKeys,
                                                                      parameter.getJoinContext().getFilter());
            Map<String, T> joinableDataMap =
                    joinableData.stream()
                                .collect(Collectors.toMap(s -> s.getRowkey(), s -> s));

            String referenceKey = joinContext.getTableName() + ":" + fromKey;

            for (Row row : rows) {
                boolean isMatched = false;
                isMatched = merge(parameter, row, joinableDataMap);

                if (!isMatched) {
                    row.dead();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private <T extends HbaseEntity> boolean merge(JoinTypeProcessorParameter parameter, Row row, Map<String, T> referenceDataMap) {

        String fromAlias = parameter.getFromAlias();
        String joinAlias = parameter.getJoinAlias();
        Field fromField = parameter.getFromField();
        Field joinField = parameter.getJoinField();

        Map<Object, List<T>> map = referenceDataMap.values().stream()
                                                   .collect(Collectors.groupingBy(s -> {
                                                       try {
                                                           return joinField.get(s);
                                                       } catch (IllegalAccessException e) {
                                                           e.printStackTrace();
                                                           return null;
                                                       }
                                                   }));

        List<T> references = row.getData(fromAlias)
                                .stream()
                                .map(s -> {
                                    try {
                                        return fromField.get(s);
                                    } catch (IllegalAccessException e) {
                                        e.printStackTrace();
                                        return null;
                                    }
                                })
                                .filter(s -> null != s)
                                .map(s -> map.get(s))
                                .filter(s -> null != s)
                                .flatMap(s -> s.stream())
                                .collect(Collectors.toList());

        if (CollectionUtils.isEmpty(references)) {
            return false;
        }

        row.addNew(joinAlias, (List<HbaseEntity>) references);
        return true;

    }

}
