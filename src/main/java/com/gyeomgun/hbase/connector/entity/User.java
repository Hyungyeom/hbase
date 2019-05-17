package com.gyeomgun.hbase.connector.entity;

import com.gyeomgun.hbase.connector.annotation.*;
import com.gyeomgun.hbase.connector.repository.BaseHbaseRepository;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.util.Date;

import static com.gyeomgun.hbase.connector.entity.User.TABLE_NAME;

@Getter
@Setter
@HbaseTable(name = TABLE_NAME)
public class User extends Reference implements HbaseEntity {
    public static final String TABLE_NAME = "user";

    @ColumnQualifier(name = "user_id")
    @Index
    private String userId;
    @ColumnQualifier(name = "name")
    private String name;
    @ColumnQualifier(name = "addr")
    private String address;
    @ColumnQualifier(name = "join_at")
    @Index
    private Date joinAt;

    @Override
    public String getSalt() {
        return String.valueOf(userId.hashCode() / BaseHbaseRepository.MAX_REGION_COUNT);
    }

    @Override
    public String getRowkey() {
        return getSalt() + userId;
    }

    @Override
    public String getRowkey(Object obj) {
        this.userId = (String)obj;
        return getRowkey();
    }
}
