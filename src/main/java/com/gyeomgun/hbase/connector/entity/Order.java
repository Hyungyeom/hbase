package com.gyeomgun.hbase.connector.entity;

import com.gyeomgun.hbase.connector.annotation.ColumnQualifier;
import com.gyeomgun.hbase.connector.annotation.HbaseTable;
import com.gyeomgun.hbase.connector.annotation.Index;
import com.gyeomgun.hbase.connector.annotation.RowkeyReference;
import com.gyeomgun.hbase.connector.repository.BaseHbaseRepository;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.util.Date;

import static com.gyeomgun.hbase.connector.entity.Order.TABLE_NAME;

@HbaseTable(name = TABLE_NAME)
@Getter
@Setter
public class Order extends Reference implements HbaseEntity {
    public static final String TABLE_NAME = "order";

    @ColumnQualifier(name = "order_id")
    private String orderId;
    @ColumnQualifier(name = "user_id")
    @Index
    @RowkeyReference(User.class)
    private String userId;
    @ColumnQualifier(name = "total_amount")
    private BigDecimal totalAmount;
    @ColumnQualifier(name = "order_at")
    @Index
    private Date orderAt;

    @Override
    public String getSalt() {
        return String.valueOf(orderId.hashCode() / BaseHbaseRepository.MAX_REGION_COUNT);
    }

    @Override
    public String getRowkey() {
        return getSalt() + orderId;
    }

    @Override
    public String getRowkey(Object obj) {
        this.orderId = (String)obj;
        return getRowkey();
    }
}
