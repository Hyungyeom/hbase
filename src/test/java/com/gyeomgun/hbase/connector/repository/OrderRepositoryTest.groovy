package com.gyeomgun.hbase.connector.repository

import com.gyeomgun.hbase.connector.config.SpringTestConfig
import com.gyeomgun.hbase.connector.context.Context
import com.gyeomgun.hbase.connector.entity.Order
import com.gyeomgun.hbase.connector.index.IndexManager
import com.gyeomgun.hbase.connector.result.ResultA
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Admin
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hbase.thirdparty.com.google.common.collect.Lists
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import spock.lang.Specification

import java.text.SimpleDateFormat
import java.util.function.Consumer

@SpringBootTest(classes = SpringTestConfig.class)
class OrderRepositoryTest extends Specification {
    @Autowired
    private OrderRepository dut
    @Autowired
    private Connection connection
    @Autowired
    private IndexManager indexManager
    @Autowired
    private HbaseRepositoryFactory factory

    List<Order> order = Lists.newArrayList()
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    def setup() {
        order = Lists.newArrayList()

        order.add(order("10000", "account1", 100l, sdf.parse("2019-01-01 10:00:00")))
        order.add(order("10001", "account2", 150l, sdf.parse("2019-01-01 11:00:00")))
        order.add(order("10002", "account1", 200l, sdf.parse("2019-01-01 12:00:00")))
        order.add(order("10003", "account2", 80l, sdf.parse("2019-01-02 11:00:00")))
        order.add(order("10004", "account1", 150l, sdf.parse("2019-01-02 20:00:00")))
        order.add(order("10005", "account2", 70l, sdf.parse("2019-01-02 22:00:00")))
        order.add(order("10006", "account1", 350l, sdf.parse("2019-01-03 07:00:00")))
        order.add(order("10007", "account2", 800l, sdf.parse("2019-01-03 20:00:00")))
        order.add(order("10008", "account1", 770l, sdf.parse("2019-01-04 08:00:00")))
        order.add(order("10009", "account2", 670l, sdf.parse("2019-01-05 10:00:00")))
        order.add(order("10010", "account1", 570l, sdf.parse("2019-01-05 12:00:00")))

        dut.save(order)
    }

    def cleanup() {
        Admin admin = connection.getAdmin()
        def tableName = TableName.valueOf(Order.TABLE_NAME)
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName)
            admin.deleteTable(tableName)
        }

        IndexManager.idxTableNames
                .stream()
                .parallel()
                .forEach(new Consumer<TableName>() {
                    @Override
                    void accept(TableName t) {
                        admin.disableTable(t)
                        admin.deleteTable(t)
                    }
                })
    }

    def "find last one"() {
        when:
        Optional<Order> actual = dut.findFirst(true)

        then:
        "10010".equals(actual.get().getOrderId())
    }

    def "find first one"() {
        when:
        Optional<Order> actual = dut.findFirst(false)

        then:
        "10000".equals(actual.get().getOrderId())
    }

    def "read index"() {
        when:
        ResultScanner scanner = indexManager.getScanner("__idx__order__order_at")

        then:
        int i = 0
        while(true) {
            Result r = scanner.next()
            if (r == null || r.isEmpty()) {
                break
            }

            byte[] rowkey = r.getRow()
            Date originalData = new Date(Bytes.toLong(rowkey, 0,8))
            String originalRowkeyWithoutSalt = Bytes.toString(rowkey, 8)

            String originalRowkeyString = Bytes.toString(r.getValue(IndexManager.BYTE_CF, IndexManager.BYTE_CQ))
            Date orderAt = originalData
            order.get(i).getOrderAt().compareTo(orderAt) == 0
            order.get(i++).getRowkey().equals(originalRowkeyString)
        }
    }

    def "read to end with index value using context"() {
        given:
        Context<Order> orderContext = new Context<>(Order.class, factory)
        Date startDate =  sdf.parse("2018-01-01 00:00:00")
        Date endDate = sdf.parse("2019-01-10 00:00:00")

        when:
        def select = orderContext.withIndex("order_at")
                                 .fromToValue(startDate, endDate)
                                 .select()

        List<Order> result = Lists.newArrayList()
        while(true) {
            ResultA resultA = select.next()

            if (resultA == null) {
                break
            }
            result.add(resultA.getData())
        }

        then:
        result.size() == order.size()
    }

    def "paging using context"() {
        given:
        Context<Order> orderContext = new Context<>(Order.class, factory)
        Date startDate =  sdf.parse("2019-01-01 00:00:00")
        Date endDate = sdf.parse("2019-01-06 00:00:00")

        when:
        def page1 = orderContext.withIndex("order_at")
                                 .fromToValue(startDate, endDate)
                                 .select()

        byte[] lastRowkey
        for(int i = 0; i < 3; i++) {
            ResultA a = page1.next()
            lastRowkey = a.indexRowkey
        }

        def page2 = orderContext.withIndex("order_at")
                                .fromRowkeyToValue(lastRowkey, endDate, false)
                                .select()
        ResultA b = page2.next()

        then:
        ((Order)b.getData()).getOrderId().equals("10003")
    }

    def order(orderId, userId, totalAmount, date) {
        Order order = new Order()
        order.setOrderId(orderId)
        order.setUserId(userId)
        order.setTotalAmount(BigDecimal.valueOf(totalAmount))
        order.setOrderAt(date)
        order
    }
}

