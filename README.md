# hbase-connector
Now you can use hbase as rdb with this connector.
- secondary index
- inner join
- outer join


## Documentation
soon

usage
```
Context<Order> orderContext = new Context<>(Order.class, factory)
        Context<OrderDetail> orderDetailContext = new Context<>(OrderDetail.class, factory)
        Context<User> userContext = new Context<>(User.class, factory)
        Date startDate =  sdf.parse("2018-01-01 00:00:00")
        Date endDate = sdf.parse("2019-01-10 00:00:00")

        JoinResultIterator resutls =
                JoinGroup.create(orderContext, "order")
                         .inner(orderDetailContext, "dtl").onByReference("order.order_id", "dtl.order_id")
                         .inner(userContext, "user").onByRowkey("order.user_id")
                         .select()
```


## Install
#### required
- Spring Boot 2.x / Java 1.8
- hbase 2.x

soon

## Contributing
soon

## License
soon
