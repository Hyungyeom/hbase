package com.gyeomgun.hbase.connector.entity;

import com.gyeomgun.hbase.connector.annotation.ColumnQualifier;
import com.gyeomgun.hbase.connector.annotation.HbaseTable;
import com.gyeomgun.hbase.connector.annotation.TableReference;
import com.gyeomgun.hbase.connector.repository.BaseHbaseRepository;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;

import static com.gyeomgun.hbase.connector.entity.OrderDetail.TABLE_NAME;


@HbaseTable(name = TABLE_NAME)
@Getter
@Setter
public class OrderDetail extends Reference implements HbaseEntity {
    public static final String TABLE_NAME = "order_dtl";

    @ColumnQualifier(name = "order_dtl_seq")
    private Long orderDetailSequence;

    @ColumnQualifier(name = "order_id")
    @TableReference(Order.class)
    private String orderId;

    @ColumnQualifier(name = "product_nm")
    private String productName;

    @ColumnQualifier(name = "qty")
    private Integer qty;

    @ColumnQualifier(name = "amount")
    private BigDecimal amount;


    @Override
    public String getSalt() {
        return String.valueOf(orderDetailSequence / BaseHbaseRepository.MAX_REGION_COUNT);
    }

    @Override
    public String getRowkey() {
        return getSalt() + orderDetailSequence;
    }

    @Override
    public String getRowkey(Object obj) {
        this.orderDetailSequence = (Long)obj;
        return getRowkey();
    }
}
