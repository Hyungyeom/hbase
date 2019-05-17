package com.gyeomgun.hbase.connector.repository;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.gyeomgun.hbase.connector.Utils;
import com.gyeomgun.hbase.connector.annotation.ColumnQualifier;
import com.gyeomgun.hbase.connector.annotation.HbaseTable;
import com.gyeomgun.hbase.connector.annotation.Index;
import com.gyeomgun.hbase.connector.annotation.TableReference;
import com.gyeomgun.hbase.connector.entity.HbaseEntity;
import com.gyeomgun.hbase.connector.index.IndexManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.gyeomgun.hbase.connector.entity.HbaseEntity.BYTE_DATA_COLUMN_FAMILY_NAME;
import static com.gyeomgun.hbase.connector.entity.HbaseEntity.BYTE_INDEX_COLUMN_FAMILY_NAME;
import static com.gyeomgun.hbase.connector.entity.Reference.BYTE_REF_COLUMN_FAMILY_NAME;

/**
 * Created by hyungyeom on 19/01/2019.
 */
@Slf4j
public abstract class BaseHbaseRepository<T extends HbaseEntity> {
    public static final int MAX_REGION_COUNT = 10;

    private IndexManager indexManager;
    private TableName tableName;
    private Table table;

    private static Map<Class, List<Field>> columnQualifierMap = Maps.newConcurrentMap();
    public static Map<Class, List<Field>> indexMap = Maps.newConcurrentMap();

    private static Function<String, byte[]> rowkeySerializer;
    private static Function<String, byte[]> columnQualifierSerializer;
    private Gson gson = new Gson();

    @PostConstruct
    public void createIndex() throws IOException {
        T t = getInstance(null);
        indexManager.createIndexTable(t, indexMap.get(t.getClass()));
    }

    public BaseHbaseRepository(Table table, IndexManager indexManager) {
        this.table = table;
        this.indexManager = indexManager;

        tableName = table.getName();
        this.table = table;
        rowkeySerializer = Bytes::toBytes;
        columnQualifierSerializer = Bytes::toBytes;
        T t = getInstance(null);
        List<Field> col = Arrays.stream(t.getClass().getDeclaredFields())
                .filter(s -> s.getAnnotation(ColumnQualifier.class) != null)
                .collect(Collectors.toList());
        columnQualifierMap.put(t.getClass(), Lists.newCopyOnWriteArrayList(col));

        List<Field> idx = Arrays.stream(t.getClass().getDeclaredFields())
                .filter(s -> s.getAnnotation(Index.class) != null)
                .collect(Collectors.toList());

        indexMap.put(t.getClass(), Lists.newCopyOnWriteArrayList(idx));
    }

    public abstract T getInstance(Map<String, List<String>> referenceMap);

    public void tableCreateIfNotExist(Admin admin) throws Exception {
        tableCreateIfNotExist(admin, Lists.newArrayList());
    }

    protected void tableCreateIfNotExist(Admin admin, Collection<ColumnFamilyDescriptor> additionalFamily)
            throws Exception {
        if (!admin.tableExists(table.getName())) {

            List<ColumnFamilyDescriptor> families = Lists.newLinkedList();
            families.add(ColumnFamilyDescriptorBuilder.newBuilder(BYTE_DATA_COLUMN_FAMILY_NAME).build());
            families.add(ColumnFamilyDescriptorBuilder.newBuilder(BYTE_REF_COLUMN_FAMILY_NAME).build());
            families.add(ColumnFamilyDescriptorBuilder.newBuilder(BYTE_INDEX_COLUMN_FAMILY_NAME).build());

            additionalFamily.stream()
                    .forEach(s -> families.add(s));

            TableDescriptor descriptor = TableDescriptorBuilder.newBuilder(table.getName())
                    .setColumnFamilies(families)
                    .build();

            admin.createTable(descriptor, getSplitArray());
        }
    }

    private byte[][] getSplitArray() {
        byte[][] result = new byte[MAX_REGION_COUNT][];
        for (int i = 0; i < MAX_REGION_COUNT; i++) {
            result[i] = Bytes.toBytes(String.valueOf(i));
        }
        return result;
    }

    public Optional<T> findBy(String rowkey) throws IOException {
        Get get = new Get(Bytes.toBytes(rowkey));

        Result result = table.get(get);

        if (result == null || result.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(map(result));
    }

    public List<T> findBy(Collection<byte[]> rowkeys) throws Exception {
        return findBy(rowkeys, null);
    }

    public List<T> findBy(Collection<byte[]> rowkeys, Filter filter) throws Exception {
        List<Get> gets = rowkeys.stream()
                .map(s -> {
                            Get g = new Get(s);
                            if (filter != null) {
                                g.setFilter(filter);
                            }
                            return g;
                        }
                )
                .collect(Collectors.toList());
        Result[] results = table.get(gets);


        if (ArrayUtils.isEmpty(results)) {
            return Lists.newArrayList();
        }

        return Arrays.stream(results)
                .filter(s -> s != null)
                .filter(s -> s.getRow() != null)
                .map(s -> {
                    try {
                        return map(s);
                    } catch (Exception e) {
                        log.error("### Exception occurred during findBy bulk mapping", e);
                        return null;
                    }
                })
                .filter(s -> null != s)
                .collect(Collectors.toList());
    }

    public Optional<T> findFirst(boolean isReversed) throws Exception {
        Scan scan = new Scan();
        scan.setMaxResultSize(1);
        scan.setLimit(1);
        scan.setReversed(isReversed);
        scan.addFamily(BYTE_DATA_COLUMN_FAMILY_NAME);

        ResultScanner scanner = table.getScanner(scan);
        Result result = scanner.next();

        if (result == null || result.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(map(result));
    }

    public ResultScanner getScanner(Scan scan) {
        try {
            return table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void save(List<T> entities) throws Exception {
        List<Put> putList = entities.stream()
                .map(s -> {
                    try {
                        return convert(s);
                    } catch (Exception ex) {
                        log.error("Exception occurred during mapping ", ex);
                        return null;
                    }
                })
                .filter(s -> null != s)
                .collect(Collectors.toList());

        table.batch(putList, null);
        addIndex(entities);
    }

    public void save(T entity) throws Exception {
        Put put = convert(entity);
        table.put(put);
        addIndex(Lists.newArrayList(entity));
    }

    public T map(Result result) {
        Map<String, byte[]> dataMap =
                convertDataMap(result.getFamilyMap(BYTE_DATA_COLUMN_FAMILY_NAME));
        Map<String, List<String>> refMap = convertRefMap(result.getFamilyMap(BYTE_REF_COLUMN_FAMILY_NAME));

        T t = getInstance(refMap);

        List<Field> fields = getColumnQualifierFields(t);

        fields.stream().forEach(s -> {
            ColumnQualifier cq = s.getAnnotation(ColumnQualifier.class);

            if (!dataMap.containsKey(cq.name())) {
                return;
            }

            s.setAccessible(true);
            byte[] bytes = dataMap.get(cq.name());

            try {
                setField(t, s, bytes);
            } catch (Exception e) {
                log.error("Exception occurred during setField", e);
            }
        });

        return t;
    }

    public List<T> map(List<Map<String, Object>> results) {
        return results.stream()
                .map(s -> {
                    try {
                        return map(s);
                    } catch (Exception ex) {
                        log.error("Exception occurred during map", ex);
                        return null;
                    }
                })
                .filter(s -> null != s)
                .collect(Collectors.toList());
    }


    public T map(Map<String, Object> result) throws IllegalAccessException {
        T t = getInstance(null);

        List<Field> fields = getColumnQualifierFields(t);

        if (CollectionUtils.isEmpty(fields)) {
            throw new RuntimeException("CQ fields is empty");
        }

        fields.stream()
                .forEach(declaredField -> {
                    ColumnQualifier cq = declaredField.getAnnotation(ColumnQualifier.class);
                    String columnName = null;

                    if (result.containsKey(cq.name())) {
                        columnName = cq.name();
                    }

                    if (result.containsKey(cq.name().toUpperCase())) {
                        columnName = cq.name().toUpperCase();
                    }

                    if (StringUtils.isEmpty(columnName)) {
                        return;
                    }

                    declaredField.setAccessible(true);

                    Object o = result.get(columnName);

                    if (null == o) {
                        return;
                    }

                    try {
                        if (o.getClass().getSimpleName().equals("BigDecimal")) {

                            if (declaredField.getType().getSimpleName().equals("Long")) {
                                declaredField.set(t, Long.valueOf(((BigDecimal) o).longValue()));
                            } else if (declaredField.getType().getSimpleName().equals("long")) {
                                declaredField.set(t, ((BigDecimal) o).longValue());
                            } else if (declaredField.getType().getSimpleName().equals("Integer")) {
                                declaredField.set(t, Integer.valueOf(((BigDecimal) o).intValue()));
                            } else if (declaredField.getType().getSimpleName().equals("int")) {
                                declaredField.set(t, ((BigDecimal) o).intValue());
                            } else {
                                declaredField.set(t, o);
                            }
                        } else {
                            declaredField.set(t, o);
                        }
                    } catch (Exception ex) {
                        log.error("Excpetion occured during field setting", ex);
                    }
                });

        return t;
    }


    private void setField(T entity, Field field, byte[] value) throws Exception {
        Utils.setField(entity, field, value);
    }

    protected List<Field> getColumnQualifierFields(T t) {
        if (columnQualifierMap.containsKey(t.getClass())) {
            return columnQualifierMap.get(t.getClass());
        }

        throw new RuntimeException("columnQualifier is empty" + t.getClass());
    }

    private Put convert(T entity) throws Exception {
        byte[] rowkey = rowkeySerializer.apply(entity.getRowkey());

        Put put = new Put(rowkey);

        List<Field> fields = getColumnQualifierFields(entity);

        if (CollectionUtils.isEmpty(fields)) {
            throw new Exception("fields is empty");
        }

        fields.stream().forEach(s -> {
            ColumnQualifier cq = s.getAnnotation(ColumnQualifier.class);
            byte[] cqName = columnQualifierSerializer.apply(cq.name());
            try {
                addColumn(put, BYTE_DATA_COLUMN_FAMILY_NAME, cqName, entity, s);
            } catch (Exception e) {
                log.error("Exception occured during addColumn", e);
            }
        });
        return put;
    }

    private void addIndex(List<T> entities) throws Exception {
        if (CollectionUtils.isEmpty(entities)) {
            return;
        }

        T representation = entities.stream().findFirst().get();
        List<Field> fields = getIndexFields(representation);
        if (CollectionUtils.isEmpty(fields)) {
            return;
        }

        String tableName = representation.getClass().getAnnotation(HbaseTable.class).name();

        Map<String, List<Put>> resultMap = Maps.newConcurrentMap();
        List<Put> indexResultPutList = Lists.newLinkedList();

        for (T entity : entities) {

            Function<Field, byte[]> func = field -> {
                try {
                    return getBytes(field, entity);
                } catch (Exception e) {
                    log.error("Could not complete getBytes. " + field.getName(), e);
                    return null;
                }
            };

            Map<String, Put> idxMap = indexManager.extractIndex(tableName, fields, func,
                    entity,
                    getRowKeyExcludeSalt(entity));

            idxMap.entrySet().stream()
                    .forEach(s -> {
                        if (!resultMap.containsKey(s.getKey())) {
                            resultMap.put(s.getKey(), Lists.newLinkedList());
                        }
                        resultMap.get(s.getKey()).add(s.getValue());

                        byte[] rowkey = rowkeySerializer.apply(entity.getRowkey());
                        Put indexCf = new Put(rowkey);
                        indexCf.addColumn(BYTE_INDEX_COLUMN_FAMILY_NAME,
                                columnQualifierSerializer.apply(s.getKey()),
                                s.getValue().getRow());
                        indexResultPutList.add(indexCf);
                    });
        }

        resultMap.entrySet()
                .stream()
                .forEach(s -> indexManager.bulkInsert(s.getKey(), s.getValue()));

        table.batch(indexResultPutList, null);
    }

    private String getRowKeyExcludeSalt(T entity) {
        return StringUtils.substringAfter(entity.getRowkey(), entity.getSalt());
    }

    private byte[] getBytes(Field field, T entity) throws Exception {
        return Utils.getBytes(field, entity);
    }

    protected List<Field> getIndexFields(T t) {
        if (indexMap.containsKey(t.getClass())) {
            return indexMap.get(t.getClass());
        }

        throw new RuntimeException("Index is empty" + t.getClass());
    }

    public List<ReferenceDetail> extractReferenceMap(T entity) throws IllegalAccessException {
        Preconditions.checkNotNull(entity.getClass().getAnnotation(HbaseTable.class),
                "HbaseTable annotation must not be null. class: " + entity.getClass());
        String refKeyTableName = entity.getClass().getAnnotation(HbaseTable.class).name();
        Field[] declaredFields = entity.getClass().getDeclaredFields();

        List<ReferenceDetail> resultList = Lists.newLinkedList();
        for (Field declaredField : declaredFields) {

            boolean hasReference =
                    Arrays.stream(declaredField.getAnnotations())
                            .anyMatch(s -> s.annotationType().equals(TableReference.class));

            if (!hasReference) {
                continue;
            }

            declaredField.setAccessible(true);
            Object value = declaredField.get(entity);

            if (value == null) {
                continue;
            }

            Preconditions.checkNotNull(declaredField.getAnnotation(ColumnQualifier.class),
                    "ColumnQualifier annotation must not be null. field: "
                            + declaredField.getName());
            String refKeyColumnName = declaredField.getAnnotation(ColumnQualifier.class).name();

            HbaseEntity hbaseEntity = null;
            try {
                hbaseEntity = declaredField.getAnnotation(TableReference.class).value()
                        .newInstance();
                hbaseEntity.getRowkey(value);
                Preconditions.checkNotNull(hbaseEntity, "Target entity must not be null");
            } catch (InstantiationException e) {
                log.error("Could not create new instance", e);
                throw new RuntimeException("Could not create new instance", e);
            }

            ReferenceDetail referenceDetail = ReferenceDetail.builder()
                    .hbaseEntity(hbaseEntity)
                    .referenceTableName(refKeyTableName)
                    .referenceColumnQualifierName(refKeyColumnName)
                    .referenceValue(entity.getRowkey())
                    .build();
            resultList.add(referenceDetail);
        }
        return resultList;
    }

    public void putReferences(List<ReferenceDetail> referenceDetailList) throws Exception {
        for (List<ReferenceDetail> referenceDetails : Lists.partition(referenceDetailList, 2000)) {

            Map<String, List<ReferenceDetail>> groupByRowkey =
                    referenceDetails.stream()
                            .collect(Collectors.groupingBy(s -> s.getHbaseEntity().getRowkey()));

            List<Put> putList = Lists.newLinkedList();

            List<Get> z = groupByRowkey.keySet().stream()
                    .map(s -> Bytes.toBytes(s))
                    .map(s -> {
                        Get g = new Get(s);
                        g.setCacheBlocks(false);
                        g.addFamily(BYTE_REF_COLUMN_FAMILY_NAME);
                        return g;
                    })
                    .collect(Collectors.toList());

            Result[] results = table.get(z);
            Map<String, Result> getMap =
                    Arrays.stream(results)
                            .filter(s -> null != s && !s.isEmpty() && StringUtils
                                    .isNotEmpty(Bytes.toString(s.getRow())))
                            .collect(Collectors.toMap(s -> Bytes.toString(s.getRow()), s -> s));

            for (Map.Entry<String, List<ReferenceDetail>> each : groupByRowkey.entrySet()) {
                Put put = convert(getMap, each.getKey(), each.getValue());
                putList.add(put);
            }

            log.info("#### Put ref size " + putList.size());
            table.batch(putList, null);
        }
    }

    private Put convert(Map<String, Result> getMap, String rowkey, List<ReferenceDetail> referenceDetails)
            throws IOException {
        Result result = getMap.get(rowkey);

        Map<String, byte[]> refMap = Maps.newHashMap();
        if (result != null && !result.isEmpty()) {
            refMap = convertDataMap(result.getFamilyMap(BYTE_REF_COLUMN_FAMILY_NAME));
        }

        Put put = new Put(Bytes.toBytes(rowkey));

        Map<String, Set<String>> refCqMap =
                referenceDetails.stream()
                        .collect(Collectors.groupingBy(s -> s.getReferenceKey()
                                , Collectors.mapping(s -> s.getReferenceValue(), Collectors.toSet())));

        for (Map.Entry<String, Set<String>> each : refCqMap.entrySet()) {
            if (refMap.containsKey(each.getKey())) {
                Type setType = new TypeToken<HashSet<String>>() {}.getType();
                Set<String> legacyRef = gson.fromJson(Bytes.toString(refMap.get(each.getKey())),
                        setType);
                each.getValue().addAll(legacyRef);
            }

            byte[] byteRefValue = Bytes.toBytes(gson.toJson(each.getValue()));
            byte[] byteRefCQ = Bytes.toBytes(each.getKey());
            put.addColumn(BYTE_REF_COLUMN_FAMILY_NAME, byteRefCQ, byteRefValue);
        }

        return put;
    }

    private void addColumn(Put put, byte[] columnFamily, byte[] columnQualifier, T entity, Field field)
            throws Exception {
        field.setAccessible(true);

        if (field.get(entity) == null) {
            return;
        }

        put.addColumn(columnFamily, columnQualifier, Utils.getBytes(field, entity));
    }

    private Map<String, List<String>> convertRefMap(NavigableMap<byte[], byte[]> familyMap) {
        Map<String, List<String>> result = Maps.newHashMap();
        for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
            Type listType = new TypeToken<ArrayList<String>>() {}.getType();
            List<String> list = gson.fromJson(Bytes.toString(entry.getValue()), listType);

            result.put(Bytes.toString(entry.getKey()), list);
        }

        return result;
    }

    private Map<String, byte[]> convertDataMap(NavigableMap<byte[], byte[]> familyMap) {
        Map<String, byte[]> result = Maps.newHashMap();
        familyMap.forEach((bytes, bytes2) -> result.put(Bytes.toString(bytes), bytes2));
        return result;
    }
}
