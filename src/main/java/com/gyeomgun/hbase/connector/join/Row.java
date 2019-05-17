package com.gyeomgun.hbase.connector.join;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.base.Preconditions;


import com.gyeomgun.hbase.connector.entity.HbaseEntity;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Row {
    private Map<String, List<HbaseEntity>> data;
    private boolean isDead = false;
    private Row(Map<String, List<HbaseEntity>> data) {
        this.data = data;
    }
    private Row(byte[] indexRowkey, Map<String, List<HbaseEntity>> data) {
        this.indexRowkey = indexRowkey;
        this.data = data;
    }
    private byte[] indexRowkey;

    public static Row createRow(Map<String, List<HbaseEntity>> data) {
        return new Row(data);
    }

    public static Row createRow(byte[] indexRowkey, Map<String, List<HbaseEntity>> data) {
        return new Row(indexRowkey, data);
    }

    public List<? extends HbaseEntity> getData(String alias) {
        Preconditions.checkState(data.containsKey(alias), "From data must not be null");
        return data.get(alias);
    }

    public <T> T findFirst(String alias, Class<T> type) {
        Optional<? extends HbaseEntity> first = getData(alias).stream().findFirst();
        if (!first.isPresent()) {
            return null;
        }
        return type.cast(first.get());
    }

    public void addNew(String alias, List<HbaseEntity> data) {
        this.data.put(alias, data);
    }

    public void dead() {
        isDead = true;
    }
}
