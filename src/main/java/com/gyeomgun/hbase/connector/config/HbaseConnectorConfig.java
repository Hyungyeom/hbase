package com.gyeomgun.hbase.connector.config;

import com.gyeomgun.hbase.connector.context.ContextFactory;
import com.gyeomgun.hbase.connector.repository.HbaseRepositoryFactory;
import com.gyeomgun.hbase.connector.index.IndexManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

/**
 * Created by hyungyeom on 19/01/2019.
 */
@Configuration
@Slf4j
public class HbaseConnectorConfig {

    @Value("${hbase.connector.zookeeper.quorum:127.0.0.1}")
    private String zookeeperQuorum;
    @Value("${hbase.connector.zookeeper.port:2181}")
    private String zookeeperClientPort;

    @Bean
    @Qualifier("hbaseConnection")
    public Connection connection() throws IOException {
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", zookeeperClientPort);
        conf.set("hbase.zookeeper.quorum", zookeeperQuorum);
        conf.set("hbase.client.retries.number", "10");
        conf.set("hbase.client.pause", "1000");
        conf.set("zookeeper.recoverty.retry", "10");

        return ConnectionFactory.createConnection(conf);
    }

    @Bean
    public HbaseRepositoryFactory hbaseRepositoryFactory() {
        return new HbaseRepositoryFactory();
    }

    @Bean
    public ContextFactory contextFactory() {
        ContextFactory contextFactory = new ContextFactory();
        contextFactory.setHbaseRepositoryFactory(hbaseRepositoryFactory());
        return contextFactory;
    }

    @Bean
    public IndexManager indexManager() throws IOException {
        return new IndexManager(connection());
    }
}
