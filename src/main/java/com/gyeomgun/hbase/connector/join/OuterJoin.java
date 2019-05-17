package com.gyeomgun.hbase.connector.join;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;


import com.gyeomgun.hbase.connector.entity.HbaseEntity;
import com.gyeomgun.hbase.connector.join.type.JoinTypeProcessor;
import com.gyeomgun.hbase.connector.join.type.JoinTypeProcessorParameter;
import com.gyeomgun.hbase.connector.repository.HbaseRepositoryFactory;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OuterJoin<T extends HbaseEntity> extends AbstractJoinNode<T> {
    public OuterJoin(HbaseRepositoryFactory factory) {
        this.factory = factory;
    }


    @Override
    public List<Row> execute(List<Row> rows) {
        List<HbaseEntity> fromData = rows.stream()
                                         .flatMap(s -> s.getData(fromAlias).stream())
                                         .collect(Collectors.toList());

        Set<byte[]> fromKeys;

        JoinTypeProcessor joinTypeProcessor = factory.getJoinTypeProcessor(joinByType);
        Preconditions.checkNotNull(joinTypeProcessor, "Could not found joinTypeProcessor. " + joinByType);

        JoinTypeProcessorParameter parameter =
                JoinTypeProcessorParameter.builder()
                                          .fromAlias(fromAlias)
                                          .joinAlias(joinAlias)
                                          .fromKey(fromKey)
                                          .joinKey(joinKey)
                                          .referenceField(referenceField)
                                          .joinContext(joinContext)
                                          .indexManager(factory.getIndexManager())
                                          .build();

        fromKeys = joinTypeProcessor.extractFromKey(parameter, fromData);

        try {
            joinTypeProcessor.mergeByRowkey(parameter, rows, fromKeys);

            if (!hasNext()) {
                return rows;
            }

            return next().execute(rows);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
