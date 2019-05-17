package com.gyeomgun.hbase.connector.result;

import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;

import com.gyeomgun.hbase.connector.join.Row;
import org.apache.commons.collections.CollectionUtils;


public class JoinResultIterator implements Iterator<Row> {
    private final Queue<Row> dataQueue = new LinkedBlockingQueue<>();
    private final Function<Integer, List<Row>> resultSupplier;

    public JoinResultIterator(Function<Integer, List<Row>> resultSupplier) {
        this.resultSupplier = resultSupplier;
    }

    @Override
    public boolean hasNext() {
        if (dataQueue.size() > 0) {
            return true;
        }

        return fillQueue();
    }

    @Override
    public Row next() {
        if (dataQueue.size() > 0) {
            return dataQueue.poll();
        }

        boolean isSuccess = fillQueue();
        if (!isSuccess) {
            return null;
        }

        return dataQueue.poll();
    }

    private boolean fillQueue() {
        List<Row> apply = resultSupplier.apply(100);
        if (CollectionUtils.isEmpty(apply)) {
            return false;
        }

        dataQueue.addAll(apply);
        return true;
    }
}
