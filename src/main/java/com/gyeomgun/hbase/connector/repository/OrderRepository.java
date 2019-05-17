package com.gyeomgun.hbase.connector.repository;

import com.gyeomgun.hbase.connector.entity.Order;
import com.gyeomgun.hbase.connector.index.IndexManager;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository
public class OrderRepository extends BaseHbaseRepository<Order> {
    public OrderRepository(@Qualifier("hbaseConnection") Connection connection,
                           @Qualifier("indexManager") IndexManager indexManager) throws Exception {
        super(connection.getTable(TableName.valueOf(Order.TABLE_NAME)), indexManager);
        tableCreateIfNotExist(connection.getAdmin());
    }

    @Override
    public Order getInstance(Map<String, List<String>> referenceMap) {
        Order order = new Order();
        order.setReferenceMap(referenceMap);
        return order;
    }
}
