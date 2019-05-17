package com.gyeomgun.hbase.connector.join;

import java.lang.reflect.Field;
import java.util.function.BiPredicate;

import com.gyeomgun.hbase.connector.context.Context;
import com.gyeomgun.hbase.connector.entity.HbaseEntity;
import com.gyeomgun.hbase.connector.join.type.JoinByType;
import com.gyeomgun.hbase.connector.repository.HbaseRepositoryFactory;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractJoinNode<T extends HbaseEntity> implements JoinNode<T> {
    protected HbaseRepositoryFactory factory;
    protected String fromAlias;
    protected String fromKey;
    protected String joinAlias;
    protected String joinKey;
    protected JoinByType joinByType;
    protected Field referenceField;

    protected Context<T> joinContext;
    protected JoinNode next;

    protected OnNode nextOn;

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public JoinNode next() {
        return next;
    }

    @Override
    public void setNext(JoinNode joinNode) {
        next = joinNode;
    }

    @Override
    public void setContext(Context<T> context) {
        joinContext = context;
    }

    @Override
    public void setKeys(String fromAlias, String fromKey, String joinAlias, String joinKey) {
        this.fromAlias = fromAlias;
        this.fromKey = fromKey;
        this.joinAlias = joinAlias;
        this.joinKey = joinKey;
    }

    @Override
    public String getJoinAlias() {
        return joinAlias;
    }

    @Override
    public Context<T> getContext() { return joinContext;}

    @Override
    public void setJoinByType(JoinByType joinByType) {
        this.joinByType = joinByType;
    }

    @Override
    public void setReferenceField(Field referenceField) {
        this.referenceField = referenceField;
    }

    private void addOnNode(OnNode onNode) {
        if (nextOn == null) {
            nextOn = onNode;
        } else {
            OnNode cursor = nextOn;
            while (cursor.hasNext()) {
                cursor = cursor.getNext();
            }
            cursor.setNext(onNode);
        }
    }

    @Override
    public void addOnConditions(BiPredicate<T, T> check) {
        addOnNode(new OnNode(check));
    }
}
