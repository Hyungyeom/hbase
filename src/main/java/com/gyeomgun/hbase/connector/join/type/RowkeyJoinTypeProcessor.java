package com.gyeomgun.hbase.connector.join.type;

import com.google.common.base.Preconditions;
import com.gyeomgun.hbase.connector.context.Context;
import com.gyeomgun.hbase.connector.entity.HbaseEntity;
import com.gyeomgun.hbase.connector.join.Row;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class RowkeyJoinTypeProcessor implements JoinTypeProcessor {
    @Override
    public JoinByType getJoinByType() {
        return JoinByType.ROWKEY;
    }

    @Override
    public <T extends HbaseEntity> Set<byte[]> extractFromKey(JoinTypeProcessorParameter parameter,
                                                              List<HbaseEntity> fromData) {
        Field referenceField = parameter.getReferenceField();
        Context<T> joinContext = parameter.getJoinContext();
        Preconditions.checkNotNull(referenceField, "Could not found referenceField");

        return fromData.stream()
                       .map(s -> {
                           try {
                               return referenceField.get(s);
                           } catch (IllegalAccessException e) {
                               log.warn(
                                       "Could not found source value for rowkeyReference. rowkey: "
                                       + s.getRowkey(), e);
                               return null;
                           }
                       })
                       .filter(s -> null != s)
                       .collect(Collectors.toSet()).stream()
                       .map(s -> {
                           try {
                               T t = joinContext.getMClass().newInstance();
                               return Bytes.toBytes(t.getRowkey(s));
                           } catch (Exception e) {
                               log.warn("Could not create new instance",
                                        e);
                               return null;
                           }
                       })
                       .filter(s -> null != s)
                       .collect(Collectors.toSet());
    }

    @Override
    public <T extends HbaseEntity> void mergeByRowkey(JoinTypeProcessorParameter parameter,
                                                      List<Row> rows,
                                                      Set<byte[]> fromKeys) {
        Context<T> joinContext = parameter.getJoinContext();
        try {
            List<T> joinableData = joinContext.getRepository().findBy(fromKeys,
                                                                      parameter.getJoinContext().getFilter());
            Map<String, T> joinableDataMap =
                    joinableData.stream()
                                .collect(Collectors.toMap(s -> s.getRowkey(), s -> s));

            boolean isMatched;

            for (Row row : rows) {

                isMatched = merge(parameter, row, joinableDataMap);

                if (!isMatched) {
                    row.dead();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private <T extends HbaseEntity> boolean merge(JoinTypeProcessorParameter parameter,
                                                  Row row,
                                                  Map<String, T> referenceDataMap) {
        String fromAlias = parameter.getFromAlias();
        Field referenceField = parameter.getReferenceField();
        Context<T> joinContext = parameter.getJoinContext();
        String joinAlias = parameter.getJoinAlias();

        List<T> references = row.getData(fromAlias)
                                .stream()
                                .map((Function<HbaseEntity, T>) s -> {
                                    try {
                                        Object o = referenceField.get(s);

                                        if (o == null) {
                                            return null;
                                        }

                                        T t = joinContext.getMClass().newInstance();
                                        String rowkey = t.getRowkey(o);

                                        return referenceDataMap.get(rowkey);

                                    } catch (Exception ex) {
                                        log.warn("Could not found..", ex);
                                        return null;
                                    }

                                })
                                .filter(s -> null != s)
                                .collect(Collectors.toList());

        if (CollectionUtils.isEmpty(references)) {
            return false;
        }

        row.addNew(joinAlias, (List<HbaseEntity>) references);
        return true;
    }
}
