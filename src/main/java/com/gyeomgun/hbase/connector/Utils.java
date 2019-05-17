package com.gyeomgun.hbase.connector;

import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.Date;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created by hyungyeom on 19/01/2019.
 */

public class Utils {
    public static <T> Stream<T> generate(Supplier<T> s, long count) {
        return StreamSupport.stream(
                new Spliterators.AbstractSpliterator<T>(count, Spliterator.SIZED) {
                    long remaining = count;

                    public boolean tryAdvance(Consumer<? super T> action) {
                        if (remaining <= 0) { return false; }
                        remaining--;
                        T t = s.get();
                        if (t == null) { return false; }
                        action.accept(t);
                        return true;
                    }
                }, false);
    }

    public static byte[] getBytes(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof Integer) {
            return Bytes.toBytes(((Integer) obj).intValue());
        }
        if (obj instanceof Long) {
            return Bytes.toBytes(((Long) obj).longValue());
        }
        if (obj instanceof Short) {
            return Bytes.toBytes(((Short) obj).shortValue());
        }
        if (obj instanceof Float) {
            return Bytes.toBytes(((Float) obj).floatValue());
        }
        if (obj instanceof Boolean) {
            return Bytes.toBytes(((Boolean) obj).booleanValue());
        }
        if (obj instanceof BigDecimal) {
            return Bytes.toBytes(obj.toString());
        }
        if (obj instanceof Date || obj instanceof java.sql.Date) {
            return Bytes.toBytes(((Date) obj).getTime());
        }
        return Bytes.toBytes(obj.toString());
    }

    public static byte[] getBytes(Field field, Object entity) throws Exception {
        field.setAccessible(true);
        Object obj = field.get(entity);
        if (obj == null) {
            return null;
        }

        Class<?> type = field.getType();
        if (type == Integer.class || type == int.class) {
            return Bytes.toBytes(((Integer) obj).intValue());
        }
        if (type == Long.class || type == long.class) {
            return Bytes.toBytes(((Long) obj).longValue());
        }
        if (type == Short.class || type == short.class) {
            return Bytes.toBytes(((Short) obj).shortValue());
        }
        if (type == Float.class || type == float.class) {
            return Bytes.toBytes(((Float) obj).floatValue());
        }
        if (type == Boolean.class || type == boolean.class) {
            return Bytes.toBytes(((Boolean) obj).booleanValue());
        }
        if (type == BigDecimal.class) {
            return Bytes.toBytes(obj.toString());
        }
        if (type == Date.class || type == java.sql.Date.class) {
            return Bytes.toBytes(((Date) obj).getTime());
        }
        return Bytes.toBytes(obj.toString());
    }

    public static void setField(Object entity, Field field, byte[] value) throws Exception {
        field.setAccessible(true);

        Class<?> type = field.getType();

        if (type == Integer.class || type == int.class) {
            field.set(entity, Bytes.toInt(value));
        } else if (type == Long.class || type == long.class) {
            field.set(entity, Bytes.toLong(value));
        } else if (type == Short.class || type == short.class) {
            field.set(entity, Bytes.toShort(value));
        } else if (type == Float.class || type == float.class) {
            field.set(entity, Bytes.toFloat(value));
        } else if (type == Boolean.class || type == boolean.class) {
            field.set(entity, Bytes.toBoolean(value));
        } else if (type == BigDecimal.class) {
            field.set(entity, new BigDecimal(Bytes.toString(value)));
        } else if (type == Date.class || type == java.sql.Date.class) {
            field.set(entity, new Date(Bytes.toLong(value)));
        } else if (type.isEnum()) {
            field.set(entity, Enum.valueOf((Class<Enum>) type, Bytes.toString(value)));
        } else {
            field.set(entity, Bytes.toString(value));
        }
    }
}
