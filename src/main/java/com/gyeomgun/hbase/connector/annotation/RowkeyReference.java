package com.gyeomgun.hbase.connector.annotation;

import com.gyeomgun.hbase.connector.entity.HbaseEntity;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface RowkeyReference {
    String name() default "";
    Class<? extends HbaseEntity> value();
}

