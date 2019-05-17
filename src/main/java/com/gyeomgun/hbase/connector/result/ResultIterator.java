package com.gyeomgun.hbase.connector.result;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import com.gyeomgun.hbase.connector.entity.HbaseEntity;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import com.google.common.collect.Lists;

public class ResultIterator<T extends HbaseEntity> {
    private static final int BULK_SIZE = 100;
    private Supplier<ResultA> resultSupplier;
    private Supplier<List<ResultA>> bulkResultSupplier;

    public ResultIterator(Collection<T> results) {
        Iterator<T> resultIterator = results.iterator();
        resultSupplier = () -> new ResultA(resultIterator.next());
        bulkResultSupplier = () -> {
            int conut = 0;
            List<ResultA> bulkList = Lists.newLinkedList();
            while (true) {
                if (!resultIterator.hasNext()) {
                    return bulkList;
                }

                bulkList.add(new ResultA(resultIterator.next()));

                if (conut++ > BULK_SIZE) {
                    return bulkList;
                }
            }
        };
    }

    public ResultIterator(ResultScanner resultScanner,
                          Function<List<Result>, List<ResultA>> mapper) {
        resultSupplier = () -> {
            try {
                Result next = resultScanner.next();
                if (next == null || next.isEmpty()) {
                    return null;
                }

                List<ResultA> map = mapper.apply(Lists.newArrayList(next));

                Optional<ResultA> one = map.stream().findFirst();

                if (!one.isPresent()) {
                    return null;
                }

                return one.get();
            } catch (Exception ex) {
                return null;
            }
        };
        bulkResultSupplier = () -> {
            try {
                Result[] next = resultScanner.next(BULK_SIZE);
                if (next == null || ArrayUtils.isEmpty(next)) {
                    return Lists.newArrayList();
                }

                List<ResultA> map = mapper.apply(Lists.newArrayList(next));

                if (CollectionUtils.isEmpty(map)) {
                    return Lists.newArrayList();
                }

                return map;
            } catch (Exception ex) {
                return Lists.newArrayList();
            }

        };
    }

//    public Stream<T> asStream(int size) {
//        return Utils.generate(() -> next(), size);
//    }

    public ResultA next() {
        return resultSupplier.get();
    }
//
//    public List<T> nextBulk() {
//        return bulkResultSupplier.get();
//    }

    public List<ResultA> nextBulk() {
        return bulkResultSupplier.get();
    }
}
