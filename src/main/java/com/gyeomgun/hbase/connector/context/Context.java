package com.gyeomgun.hbase.connector.context;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.gyeomgun.hbase.connector.Utils;
import com.gyeomgun.hbase.connector.annotation.ColumnQualifier;
import com.gyeomgun.hbase.connector.annotation.HbaseTable;
import com.gyeomgun.hbase.connector.annotation.Index;
import com.gyeomgun.hbase.connector.entity.HbaseEntity;
import com.gyeomgun.hbase.connector.index.IndexManager;
import com.gyeomgun.hbase.connector.repository.BaseHbaseRepository;
import com.gyeomgun.hbase.connector.repository.HbaseRepositoryFactory;
import com.gyeomgun.hbase.connector.result.ResultA;
import com.gyeomgun.hbase.connector.result.ResultIterator;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;


import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import static com.gyeomgun.hbase.connector.entity.HbaseEntity.BYTE_DATA_COLUMN_FAMILY_NAME;

@Slf4j
public class Context<T extends HbaseEntity> {
    @Setter
    @Getter
    private String alias;
    @Getter
    private Class<T> mClass;

    @Getter
    private BaseHbaseRepository<T> repository;
    @Getter
    private Filter filter;

    @Setter
    private ResultScanner indexScanner;
    @Getter
    private HbaseRepositoryFactory factory;
    @Setter
    private byte[] indexFamily;

    public Context(Class<T> classType, HbaseRepositoryFactory factory) {
        this.mClass = classType;
        repository = factory.getRepository(classType);
        this.factory = factory;
    }

    public Context filter(Filter filter) {
        this.filter = filter;
        return this;
    }

    public String getTableName() {
        return mClass.getAnnotation(HbaseTable.class).name();
    }

    public WithIndexBuilder withIndex(String key) {
        String tableName = mClass.getAnnotation(HbaseTable.class).name();

        boolean hasIndexKey = Arrays.stream(mClass.getDeclaredFields())
                .filter(s -> s.getAnnotation(Index.class) != null)
                .anyMatch(s -> StringUtils.isNotEmpty(s.getAnnotation(Index.class).name()) ?
                        s.getAnnotation(Index.class).name().equals(key) :
                        s.getAnnotation(ColumnQualifier.class).name().equals(key));

        Preconditions.checkState(hasIndexKey, "Index not found " + key);

        String indexTableName = factory.getIndexManager()
                .createIndexTableName(tableName, key);

        return new WithIndexBuilder(indexTableName, this);
    }

    public WithIndexBuilder withIndex(String key, byte[] familyName) {
        String tableName = mClass.getAnnotation(HbaseTable.class).name();

        boolean hasIndexKey = Arrays.stream(mClass.getDeclaredFields())
                .filter(s -> s.getAnnotation(Index.class) != null)
                .anyMatch(s -> StringUtils.isNotEmpty(s.getAnnotation(Index.class).name()) ?
                        s.getAnnotation(Index.class).name().equals(key) :
                        s.getAnnotation(ColumnQualifier.class).name().equals(key));

        Preconditions.checkState(hasIndexKey, "Index not found " + key);

        String indexTableName = factory.getIndexManager()
                .createIndexTableName(tableName, key);

        return new WithIndexBuilder(indexTableName, familyName, this);
    }

    public static class WithIndexBuilder<T extends HbaseEntity> {
        private Context<T> context;
        private String indexTableName;
        private byte[] family;
        public WithIndexBuilder(String indexTableName, Context<T> context) {
            this.indexTableName = indexTableName;
            this.context = context;
        }

        public WithIndexBuilder(String indexTableName, byte[] family, Context<T> context) {
            this.indexTableName = indexTableName;
            this.context = context;
            this.context.setIndexFamily(family);
            this.family = family;
        }

        public Context fromValue(Object from) {
            byte[] fromByte = Utils.getBytes(from);
            ResultScanner scanner = context.factory.getIndexManager()
                    .getScanner(indexTableName, family, fromByte, fromByte, false);
            context.setIndexScanner(scanner);
            return context;
        }

        public Context fromToValue(Object from, Object to) {
            byte[] fromByte = Utils.getBytes(from);
            byte[] toByte = Utils.getBytes(to);
            ResultScanner scanner = context.factory.getIndexManager()
                    .getScanner(indexTableName, family, fromByte, toByte, false);
            context.setIndexScanner(scanner);
            return context;
        }

        public Context fromRowKey(byte[] from) {
            ResultScanner scanner = context.factory.getIndexManager()
                    .getScanner(indexTableName, from, true);
            context.setIndexScanner(scanner);
            return context;
        }

        public Context fromRowkeyToValue(byte[] from, Object to, boolean startRowkeyInclude) {
            byte[] toByte = Utils.getBytes(to);
            ResultScanner scanner = context.factory.getIndexManager()
                    .getScanner(indexTableName, from, toByte, true, startRowkeyInclude);
            context.setIndexScanner(scanner);
            return context;
        }

        public String getIndexTableName() {
            return indexTableName;
        }
    }

    public ResultIterator<T> select() throws IOException {
        if (indexScanner != null) {
            if (indexFamily == null) {
                indexFamily = IndexManager.BYTE_CF;
            }

            ResultIterator<T> resultIterator = new ResultIterator<>(indexScanner, result -> {
                try {

                    List<byte[]> rowkeys = result.stream()
                            .map(s -> s.getValue(indexFamily, IndexManager.BYTE_CQ))
                            .collect(Collectors.toList());


                    List<T> data = repository.findBy(rowkeys, filter);

                    if (CollectionUtils.isEmpty(data)) {
                        log.debug("Could not found raw data from index. ");
                        //todo: index 삭제
                        return null;
                    }

                    Map<String, byte[]> collect = result.stream()
                            .collect(Collectors.toMap(s -> Bytes.toString(s.getValue(indexFamily, IndexManager.BYTE_CQ)),
                                    s -> s.getRow()));

                    return data.stream()
                            .map(s -> new ResultA(collect.get(s.getRowkey()), s))
                            .collect(Collectors.toList());

                } catch (Exception ex) {
                    log.error("could not mapping", ex);
                    return null;
                }
            });
            return resultIterator;
        } else {
            Scan scan = new Scan();
            scan.addFamily(BYTE_DATA_COLUMN_FAMILY_NAME);
            scan.setReversed(false);

            ResultScanner scanner = repository.getScanner(scan);
            ResultIterator<T> resultIterator = new ResultIterator<T>(scanner, result -> {
                try {
                    return result.stream()
                            .map(s -> {
                                try {
                                    return new ResultA(repository.map(s));
                                } catch (Exception e) {
                                    log.error("### Exception occurred during Context select mapping", e);
                                    return null;
                                }
                            })
                            .filter(s -> null != s)
                            .collect(Collectors.toList());
                } catch (Exception ex) {
                    log.error("could not mapping", ex);
                    return null;
                }
            });
            return resultIterator;
        }
    }
}


