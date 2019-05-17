package com.gyeomgun.hbase.connector.repository;

import com.gyeomgun.hbase.connector.entity.OrderDetail;
import com.gyeomgun.hbase.connector.index.IndexManager;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository
public class OrderDetailRepository extends BaseHbaseRepository<OrderDetail> {
    public OrderDetailRepository(@Qualifier("hbaseConnection") Connection connection,
                           @Qualifier("indexManager") IndexManager indexManager) throws Exception {
        super(connection.getTable(TableName.valueOf(OrderDetail.TABLE_NAME)), indexManager);
        tableCreateIfNotExist(connection.getAdmin());
    }

    @Override
    public OrderDetail getInstance(Map<String, List<String>> referenceMap) {
        OrderDetail order = new OrderDetail();
        order.setReferenceMap(referenceMap);
        return order;
    }
}
