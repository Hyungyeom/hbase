package com.gyeomgun.hbase.connector.index;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.gyeomgun.hbase.connector.annotation.ColumnQualifier;
import com.gyeomgun.hbase.connector.annotation.HbaseTable;
import com.gyeomgun.hbase.connector.annotation.Index;
import com.gyeomgun.hbase.connector.entity.HbaseEntity;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.glassfish.jersey.internal.guava.Sets;
import org.springframework.beans.factory.annotation.Qualifier;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.gyeomgun.hbase.connector.entity.HbaseEntity.BYTE_DATA_COLUMN_FAMILY_NAME;

/**
 * Created by hyungyeom on 19/01/2019.
 */
@Slf4j
public class IndexManager {
    public static final Map<String, IndexDefinition> idxDefMap = Maps.newConcurrentMap();
    private static final byte[] START_ROW_KEY_SUFFIX = new byte[]{0};
    private static final byte[] STOP_ROW_KEY_SUFFIX = new byte[]{Byte.MAX_VALUE};
    private static final String DEFAULT_COLUMN_FAMILY_NAME = "index";
    public static final byte[] BYTE_CF = Bytes.toBytes(DEFAULT_COLUMN_FAMILY_NAME);
    private static final String DEFAULT_COLUMN_QUALIFIER_NAME = "rowkey";
    public static final byte[] BYTE_CQ = Bytes.toBytes(DEFAULT_COLUMN_QUALIFIER_NAME);
    public static Set<TableName> idxTableNames = Sets.newHashSet();
    private Connection connection;
    private Admin admin;

    public IndexManager(@Qualifier("hbaseConnection") Connection connection) throws IOException {
        this.connection = connection;
        admin = connection.getAdmin();

        idxTableNames = Arrays.stream(admin.listTableNames())
                .collect(Collectors.toSet());
    }

    public <T extends HbaseEntity> void createIndexTable(T target, List<Field> indexFields) throws IOException {
        String tableName = target.getClass().getAnnotation(HbaseTable.class).name();

        for (Field indexField : indexFields) {
            String cqName = indexField.getAnnotation(ColumnQualifier.class).name();
            String indexTableName = createIndexTableName(tableName, cqName);

            TableName table = TableName.valueOf(indexTableName);
            Class<? extends IndexDefinition> definition = indexField.getAnnotation(Index.class)
                    .definition();
            IndexDefinition indexDefinition = null;
            try {
                indexDefinition = definition.newInstance();
                idxDefMap.put(indexTableName, indexDefinition);
            } catch (Exception ex) {
            }

            if (!idxTableNames.contains(table)) {
                TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(table);

                List<ColumnFamilyDescriptor> families = Lists.newLinkedList();
                families.add(ColumnFamilyDescriptorBuilder.newBuilder(BYTE_CF).build());

                if (indexDefinition != null) {
                    for (Object family : indexDefinition.getFamilies()) {
                        byte[] aa = (byte[]) family;
                        families.add(ColumnFamilyDescriptorBuilder.newBuilder(aa).build());
                    }
                }
                builder.setColumnFamilies(families);
                admin.createTable(builder.build());
                idxTableNames.add(table);
            }
        }
    }

    public <T extends HbaseEntity> Collection<byte[]> getTargetFamilyNames(T target, String indexTableName) {
        if (!idxDefMap.containsKey(indexTableName)) {
            return Lists.newArrayList();
        }

        return idxDefMap.get(indexTableName).getTargetFamilies(target);
    }

    private Table instanceOf(String name) throws Exception {
        return connection.getTable(TableName.valueOf(name));
    }

    public void addIndex(String tableName, String idxName, byte[] pk, String sourceRowkey) throws Exception {
        String indexTableName = createIndexTableName(tableName, idxName);
        try (Table table = instanceOf(indexTableName)) {
            byte[] byteSourceRowKey = Bytes.toBytes(sourceRowkey);
            byte[] rowkey = ArrayUtils.addAll(pk, byteSourceRowKey);
            Put put = new Put(rowkey);
            put.addColumn(BYTE_CF, BYTE_CQ, pk);
            table.put(put);
        }
    }

    public void bulkInsert(String indexTableName, List<Put> puts) {
        try (Table table = instanceOf(indexTableName)) {
            table.put(puts);
        } catch (Exception ex) {
            log.error("Could not insert data. tableName: " + indexTableName, ex);
        }
    }

    public String createIndexTableName(String tableName, String idxName) {
        return "__idx" + "__" + tableName + "__" + idxName;
    }

    public <T extends HbaseEntity> Map<String, Put> extractIndex(String tableName, List<Field> fields,
                                                                 Function<Field, byte[]> func,
                                                                 T entity, String sequence) {

        Map<String, Put> resultMap = Maps.newHashMap();

        for (Field declaredField : fields) {
            Index idx = declaredField.getAnnotation(Index.class);
            String idxName = idx.name();

            if (StringUtils.isEmpty(idxName)) {
                idxName = declaredField.getAnnotation(ColumnQualifier.class).name();
            }

            declaredField.setAccessible(true);
            byte[] pk = func.apply(declaredField);

            if (ArrayUtils.isEmpty(pk)) {
                continue;
            }

            String indexTableName = createIndexTableName(tableName, idxName);
            byte[] byteSourceRowKey = Bytes.toBytes(entity.getRowkey());
            byte[] byteSequence = Bytes.toBytes(sequence);
            byte[] rowkey = ArrayUtils.addAll(pk, byteSequence);
            Put put = new Put(rowkey);
            put.addColumn(BYTE_CF, BYTE_CQ, byteSourceRowKey);

            Collection<byte[]> aa = getTargetFamilyNames(entity, indexTableName);
            for (byte[] cf : aa) {
                put.addColumn(cf, BYTE_CQ, byteSourceRowKey);
            }
            resultMap.put(indexTableName, put);

        }
        return resultMap;
    }

    public Result get(String tableName, byte[] rowkey) throws Exception {
        Table table = instanceOf(tableName);
        Get get = new Get(rowkey);
        return table.get(get);
    }

    public ResultScanner getScanner(String indexTableName) {
        try (Table table = instanceOf(indexTableName)) {
            Scan scan = new Scan();
            scan.setReversed(false);
            scan.addColumn(BYTE_CF, BYTE_CQ);
            return table.getScanner(scan);
        } catch (Exception e) {
            log.error("[getScanner] could not getScanner " + indexTableName, e);
            return null;
        }
    }

    public ResultScanner getScanner(String indexTableName, byte[] fromByte, boolean isFromRowkey) {
        try (Table table = instanceOf(indexTableName)) {
            Scan scan = new Scan();
            if (isFromRowkey) {
                scan.withStartRow(fromByte);
            } else {
                byte[] start = ArrayUtils.addAll(fromByte, START_ROW_KEY_SUFFIX);
                scan.withStartRow(start);
            }
            scan.addColumn(BYTE_CF, BYTE_CQ);
            return table.getScanner(scan);
        } catch (Exception e) {
            log.error("[getScanner] could not getScanner " + indexTableName, e);
            return null;
        }
    }

    public ResultScanner getScanner(String indexTableName, byte[] fromByte, byte[] toByte,
                                    boolean isFromRowkey, boolean isFromRowkeyInclude) {
        try (Table table = instanceOf(indexTableName)) {
            Scan scan = new Scan();
            scan.setCaching(100);

            if (isFromRowkey) {
                scan.withStartRow(fromByte, isFromRowkeyInclude);
            } else {
                byte[] start = ArrayUtils.addAll(fromByte, START_ROW_KEY_SUFFIX);
                scan.withStartRow(start);
            }

            byte[] stop = ArrayUtils.addAll(toByte, STOP_ROW_KEY_SUFFIX);
            scan.withStopRow(stop, true);

            scan.addColumn(BYTE_CF, BYTE_CQ);
            ResultScanner scanner = table.getScanner(scan);

            return scanner;

        } catch (Exception e) {
            log.error("[getScanner] could not getScanner " + indexTableName, e);
            return null;
        }
    }

    public ResultScanner getScanner(String indexTableName, byte[] family, byte[] fromByte, byte[] toByte,
                                    boolean isFromRowkey) {
        try (Table table = instanceOf(indexTableName)) {
            Scan scan = new Scan();

            if (null != family) {
                scan.addColumn(family, BYTE_CQ);
            } else {
                scan.addColumn(BYTE_CF, BYTE_CQ);
            }
            scan.setCaching(100);

            if (isFromRowkey) {
                scan.withStartRow(fromByte);
            } else {
                byte[] start = ArrayUtils.addAll(fromByte, START_ROW_KEY_SUFFIX);
                scan.withStartRow(start);
            }

            byte[] stop = ArrayUtils.addAll(toByte, STOP_ROW_KEY_SUFFIX);
            scan.withStopRow(stop);

            return table.getScanner(scan);
        } catch (Exception e) {
            log.error("[getScanner] could not getScanner " + indexTableName, e);
            return null;
        }
    }

    public ResultScanner getScanner(String indexTableName, Collection<byte[]> rowkeyPrefix) {
        try (Table table = instanceOf(indexTableName)) {

            Scan scan = new Scan();
            scan.setCaching(100);

            byte[] start = rowkeyPrefix.stream().sorted((s1, s2) -> Bytes.compareTo(s1, s2)).findFirst().get();
            scan.withStartRow(start);
            byte[] stop = rowkeyPrefix.stream().sorted((s1, s2) -> Bytes.compareTo(s2, s1)).findFirst().get();
            scan.withStopRow(ArrayUtils.addAll(stop, STOP_ROW_KEY_SUFFIX));

            FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ONE);
            rowkeyPrefix.stream()
                    .forEach(s -> filters.addFilter(new PrefixFilter(s)));

            scan.setFilter(filters);
            scan.addColumn(BYTE_CF, BYTE_CQ);
            return table.getScanner(scan);
        } catch (Exception e) {
            log.error("[getScanner] could not getScanner " + indexTableName, e);
            return null;
        }
    }
}
