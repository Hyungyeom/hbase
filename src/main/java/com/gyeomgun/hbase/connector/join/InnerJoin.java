package com.gyeomgun.hbase.connector.join;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.gyeomgun.hbase.connector.entity.HbaseEntity;
import com.gyeomgun.hbase.connector.join.type.JoinTypeProcessor;
import com.gyeomgun.hbase.connector.join.type.JoinTypeProcessorParameter;
import com.gyeomgun.hbase.connector.repository.HbaseRepositoryFactory;
import org.apache.commons.collections.CollectionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InnerJoin<T extends HbaseEntity> extends AbstractJoinNode<T> {
    public InnerJoin(HbaseRepositoryFactory factory) {
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


            List<Row> results = rows.stream()
                                    .filter(s -> !s.isDead())
                                    .collect(Collectors.toList());

            if (CollectionUtils.isEmpty(results)) {
                return results;
            }

            if (!hasNext()) {
                return results;
            }
            return next().execute(results);

        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
