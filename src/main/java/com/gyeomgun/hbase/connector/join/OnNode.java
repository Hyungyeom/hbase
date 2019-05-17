package com.gyeomgun.hbase.connector.join;

import com.gyeomgun.hbase.connector.entity.HbaseEntity;
import lombok.Getter;
import lombok.Setter;

import java.util.function.BiPredicate;
import java.util.function.Predicate;

public class OnNode<T extends HbaseEntity> {
    private String alias1;
    private String alias2;

    private Predicate<T> singleCondition;
    private BiPredicate<T, T> doubleCondition;

    @Setter
    @Getter
    private OnNode<T> next;

    public OnNode(BiPredicate<T, T> check) {
        this.doubleCondition = check;
    }

    public boolean hasNext() {
        return next != null;
    }

    public boolean check(T from, T s) {
        if (!doubleCondition.test(from, s)) {
            return false;
        }

        if (next == null) {
            return true;
        }

        return next.check(from, s);
    }
}
