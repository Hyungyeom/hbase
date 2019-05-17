package com.gyeomgun.hbase.connector.repository;

import com.google.common.collect.Maps;
import com.gyeomgun.hbase.connector.index.IndexManager;
import com.gyeomgun.hbase.connector.join.type.*;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by hyungyeom on 19/01/2019.
 */
public class HbaseRepositoryFactory {

    @Autowired
    private List<BaseHbaseRepository> repositories;
    @Autowired
    private IndexManager indexManager;

    private Map<Class, BaseHbaseRepository> repositoryMap = Maps.newHashMap();
    private Map<JoinByType, JoinTypeProcessor> joinTypeProcessors = Maps.newConcurrentMap();

    @PostConstruct
    public void initialize() {
        repositoryMap = repositories.stream()
                .collect(Collectors.toMap(s -> s.getInstance(null).getClass(), s -> s));

        IndexJoinTypeProcessor indexJoinTypeProcessor = new IndexJoinTypeProcessor();
        RowkeyJoinTypeProcessor rowkeyJoinTypeProcessor = new RowkeyJoinTypeProcessor();
        ReferenceJoinTypeProcessor referenceJoinTypeProcessor = new ReferenceJoinTypeProcessor();

        joinTypeProcessors.put(indexJoinTypeProcessor.getJoinByType(), indexJoinTypeProcessor);
        joinTypeProcessors.put(rowkeyJoinTypeProcessor.getJoinByType(), rowkeyJoinTypeProcessor);
        joinTypeProcessors.put(referenceJoinTypeProcessor.getJoinByType(), referenceJoinTypeProcessor);
    }

    public IndexManager getIndexManager() {
        return indexManager;
    }

    public BaseHbaseRepository getRepository(Class clazz) {
        return repositoryMap.get(clazz);
    }

    public JoinTypeProcessor getJoinTypeProcessor(JoinByType joinByType) {
        return joinTypeProcessors.get(joinByType);
    }
}
