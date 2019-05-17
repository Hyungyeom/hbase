package com.gyeomgun.hbase.connector.join;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

import com.gyeomgun.hbase.connector.annotation.ColumnQualifier;
import com.gyeomgun.hbase.connector.annotation.RowkeyReference;
import com.gyeomgun.hbase.connector.annotation.TableReference;
import com.gyeomgun.hbase.connector.context.Context;
import com.gyeomgun.hbase.connector.entity.HbaseEntity;
import com.gyeomgun.hbase.connector.join.type.JoinByType;
import com.gyeomgun.hbase.connector.result.JoinResultIterator;
import com.gyeomgun.hbase.connector.result.ResultA;
import com.gyeomgun.hbase.connector.result.ResultIterator;
import org.apache.commons.collections.CollectionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;


import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

@Slf4j
public class JoinGroup<T extends HbaseEntity> {

    @Getter
    private Context<T> from;
    private Collection<T> fromData;
    @Setter
    private JoinNode joinNode;

    private JoinGroup(Context<T> from, String alias) {
        this.from = from;
        from.setAlias(alias);
    }

    private JoinGroup(Context<T> from, String alias, Collection<T> data) {
        this.from = from;
        from.setAlias(alias);
        this.fromData = data;
    }

    public static <T extends HbaseEntity> JoinGroup create(Context<T> from, String alias) {
        return new JoinGroup(from, alias);
    }

    public static <T extends HbaseEntity> JoinGroup from(Context<T> from, String alias, Collection<T> data) {
        return new JoinGroup(from, alias, data);


    }

    protected void addJoinNode(JoinNode node) {
        if (joinNode == null) {
            joinNode = node;
        } else {
            JoinNode cursor = joinNode;
            while (cursor.hasNext()) {
                cursor = cursor.next();
            }
            cursor.setNext(node);
        }
    }

    public JoinResultIterator select() throws Exception {
        Map<String, List<HbaseEntity>> result = Maps.newHashMap();
        final ResultIterator<T> select;

        if (null != fromData) {
            select = new ResultIterator<>(fromData);
        } else {
            select = from.select();
        }

        //todo
        return new JoinResultIterator(count -> {
            try {

                List<ResultA> driveData = select.nextBulk();
                if (CollectionUtils.isEmpty(driveData)) {
                    log.info("### index value is null");
                    return null;
                }

                List<Row> row =
                driveData.stream()
                         .map(s -> {
                             Map<String, List<HbaseEntity>> data = Maps.newHashMap();
                             data.put(from.getAlias(), Lists.newArrayList(s.getData()));
                             return Row.createRow(s.getIndexRowkey(), data); })
                         .collect(Collectors.toList());


                if (joinNode == null) {
                    return row;
                }

                return joinNode.execute(row);
            } catch (Exception e) {
                log.error("### Could not select join", e);
                return null;
            }
        });
    }

    public OnBuilder outer(Context<T> join, String alias) {
        join.setAlias(alias);
        return new OnBuilder(new OuterJoin<T>(from.getFactory()), join, this);
    }

    public OnBuilder inner(Context<T> join, String alias) {
        join.setAlias(alias);
        return new OnBuilder(new InnerJoin<T>(from.getFactory()), join, this);
    }

    public static class OnBuilder<T extends HbaseEntity> {
        private JoinGroup joinGroup;
        private Context<T> join;
        private JoinNode<T> joinNode;

        public OnBuilder(JoinNode<T> joinNode, Context<T> join, JoinGroup joinGroup) {
            this.join = join;
            this.joinGroup = joinGroup;
            this.joinNode = joinNode;
        }

        public JoinGroupAfterOn onByReference(String fromKey, String joinKey) {
            Preconditions.checkArgument(fromKey.split("\\.").length == 2,
                    "Alias and key must be split by Dot(.)");
            Preconditions.checkArgument(joinKey.split("\\.").length == 2,
                    "Alias and key must be split by Dot(.)");

            String fromAlias = fromKey.split("\\.")[0];
            String fromK = fromKey.split("\\.")[1];
            joinNode.setKeys(fromAlias, fromK,
                    joinKey.split("\\.")[0], joinKey.split("\\.")[1]);
            joinNode.setContext(join);
            joinNode.setJoinByType(JoinByType.REFERENCE);
            joinGroup.addJoinNode(joinNode);
            return new JoinGroupAfterOn(joinGroup, joinNode);
        }

        public JoinGroupAfterOn onByRowkey(String fromKey) {
            Preconditions.checkArgument(fromKey.split("\\.").length == 2,
                                        "Alias and key must be split by Dot(.)");
            String fromAlias = fromKey.split("\\.")[0];
            String fromK = fromKey.split("\\.")[1];
            String joinAlias = join.getAlias();

            joinNode.setKeys(fromAlias, fromK, joinAlias, null);
            joinNode.setContext(join);
            joinNode.setJoinByType(JoinByType.ROWKEY);
            joinGroup.addJoinNode(joinNode);

            Class fromClass = null;
            JoinNode n = joinGroup.joinNode;
            while(true) {
                if (fromAlias.equals(n.getJoinAlias())) {
                    fromClass = n.getContext().getMClass();
                    break;
                }
                if (!n.hasNext()) {
                    break;
                }
                n = n.next();
            }

            if (fromClass == null) {
                fromClass = joinGroup.getFrom().getMClass();
            }

            joinNode.setReferenceField(findReferenceField(fromK, fromClass));
            return new JoinGroupAfterOn(joinGroup, joinNode);
        }

        public JoinGroupAfterOn onByIndex(String fromKey, String joinKey) {
            Preconditions.checkArgument(fromKey.split("\\.").length == 2,
                                        "Alias and key must be split by Dot(.)");
            Preconditions.checkArgument(joinKey.split("\\.").length == 2,
                                        "Alias and key must be split by Dot(.)");

            String fromAlias = fromKey.split("\\.")[0];
            String fromK = fromKey.split("\\.")[1];
            joinNode.setKeys(fromAlias, fromK,
                             joinKey.split("\\.")[0], joinKey.split("\\.")[1]);
            joinNode.setContext(join);
            joinNode.setJoinByType(JoinByType.INDEX);
            joinGroup.addJoinNode(joinNode);

            return new JoinGroupAfterOn(joinGroup, joinNode);
        }

        private Field findReferenceField(String fromKey, Class clazz) {
            Optional<Field> rowkeyRefField =
                    Arrays.stream(clazz.getDeclaredFields())
                          .filter(s -> s.getAnnotation(RowkeyReference.class) != null || s.getAnnotation(TableReference.class) != null)
                          .filter(s -> fromKey.equals(s.getAnnotation(ColumnQualifier.class).name()))
                          .findFirst();

            Preconditions.checkState(rowkeyRefField.isPresent(), "Could not found rowkeyReference field. " + fromKey);
            Field field = rowkeyRefField.get();
            field.setAccessible(true);
            return field;
        }
    }

    public static class JoinGroupAfterOn<T extends HbaseEntity> extends JoinGroup {
        private JoinNode lastJoinNode;
        private JoinGroupAfterOn(JoinGroup joinGroup, JoinNode lastJoinNode) {
            super(joinGroup.from, joinGroup.from.getAlias(), joinGroup.fromData);
            setJoinNode(joinGroup.joinNode);

            this.lastJoinNode = lastJoinNode;
        }

        public JoinGroup and(BiPredicate<T, T> check) {
            this.lastJoinNode.addOnConditions(check);
            return this;
        }
    }

        //todo
        public JoinGroup or(String fromKey, String joinKey) {
            throw new NotImplementedException();
        }
    }


