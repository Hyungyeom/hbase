package com.gyeomgun.hbase.connector.result;

import com.gyeomgun.hbase.connector.entity.HbaseEntity;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ResultA<T extends HbaseEntity> {
    private byte[] indexRowkey;
    private T data;

    public ResultA(T data) {
        this.data = data;
    }

    public ResultA(byte[] indexRowkey, T data) {
        this.indexRowkey = indexRowkey;
        this.data = data;
    }
}
