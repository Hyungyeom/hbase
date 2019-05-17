package com.gyeomgun.hbase.connector.context;

import com.gyeomgun.hbase.connector.entity.HbaseEntity;
import com.gyeomgun.hbase.connector.repository.HbaseRepositoryFactory;
import lombok.Setter;

/**
 * Created by hyungyeom on 19/01/2019.
 */
public class ContextFactory {
    @Setter
    private HbaseRepositoryFactory hbaseRepositoryFactory;

    public <T extends HbaseEntity> Context<T> createContext(Class<T> t) {
        return new Context<>(t, hbaseRepositoryFactory);
    }

}
