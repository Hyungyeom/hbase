package com.gyeomgun.hbase.connector.repository;

import com.gyeomgun.hbase.connector.annotation.HbaseTable;
import com.gyeomgun.hbase.connector.entity.HbaseEntity;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class ReferenceDetail {
    private String targetTableName;
    private HbaseEntity hbaseEntity;
    private String referenceKey;
    private String referenceValue;

    public static ReferenceDetailBuilder builder() {
        return new ReferenceDetailBuilder();
    }

    static class ReferenceDetailBuilder {
        private String refTableName;
        private HbaseEntity refHbaseEntity;
        private String refColumnQualifierName;
        private String refValue;

        public ReferenceDetailBuilder hbaseEntity(HbaseEntity hbaseEntity) {
            refHbaseEntity = hbaseEntity;
            return this;
        }

        public ReferenceDetailBuilder referenceTableName(String value) {
            refTableName = value;
            return this;
        }

        public ReferenceDetailBuilder referenceColumnQualifierName(String value) {
            refColumnQualifierName = value;
            return this;
        }

        public ReferenceDetailBuilder referenceValue(String value) {
            refValue = value;
            return this;
        }

        public ReferenceDetail build() {
            String key = refTableName + ":" + refColumnQualifierName;
            String targetTableName = refHbaseEntity.getClass().getAnnotation(HbaseTable.class).name();
            return new ReferenceDetail(targetTableName, refHbaseEntity, key, refValue);
        }
    }
}
