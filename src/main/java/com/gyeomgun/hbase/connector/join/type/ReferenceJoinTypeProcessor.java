package com.gyeomgun.hbase.connector.join.type;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.gyeomgun.hbase.connector.context.Context;
import com.gyeomgun.hbase.connector.entity.HbaseEntity;
import com.gyeomgun.hbase.connector.entity.Reference;
import com.gyeomgun.hbase.connector.join.Row;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.util.Bytes;


public class ReferenceJoinTypeProcessor implements JoinTypeProcessor {
    @Override
    public JoinByType getJoinByType() {
        return JoinByType.REFERENCE;
    }

    @Override
    public <T extends HbaseEntity> Set<byte[]> extractFromKey(JoinTypeProcessorParameter parameter,
                                                              List<HbaseEntity> fromData) {
        Context<T> joinContext = parameter.getJoinContext();
        String referenceKey = joinContext.getTableName() + ":" + parameter.getFromKey();

        return fromData.stream()
                       .flatMap(s -> ((Reference) s).getReferenceMap()
                                                    .get(referenceKey)
                                                    .stream())
                       .collect(Collectors.toSet()).stream()
                       .map(s -> Bytes.toBytes(s))
                       .collect(Collectors.toSet());
    }

    @Override
    public <T extends HbaseEntity> void mergeByRowkey(JoinTypeProcessorParameter parameter, List<Row> rows,
                                                      Set<byte[]> fromKeys) {
        try {
            Context<T> joinContext = parameter.getJoinContext();
            List<T> joinableData = joinContext.getRepository().findBy(fromKeys,
                                                                      parameter.getJoinContext().getFilter());
            Map<String, T> joinableDataMap =
                    joinableData.stream()
                                .collect(Collectors.toMap(s -> s.getRowkey(), s -> s));

            String referenceKey = joinContext.getTableName() + ":" + parameter.getFromKey();

            for (Row row : rows) {
                boolean isMatched = false;
                isMatched = merge(parameter, row, referenceKey, joinableDataMap);

                if (!isMatched) {
                    row.dead();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private <T extends HbaseEntity> boolean merge(JoinTypeProcessorParameter parameter, Row row, String key,
                                                  Map<String, T> referenceDataMap) {
        String fromAlias = parameter.getFromAlias();
        String joinAlias = parameter.getJoinAlias();

        List<T> references = row.getData(fromAlias)
                                .stream()
                                .flatMap(s -> ((Reference) s).getReferenceMap().get(key).stream())
                                .map(s -> referenceDataMap.get(s))
                                .filter(s -> null != s)
                                .collect(Collectors.toList());

        if (CollectionUtils.isEmpty(references)) {
            return false;
        }

        row.addNew(joinAlias, (List<HbaseEntity>) references);
        return true;
    }
}
