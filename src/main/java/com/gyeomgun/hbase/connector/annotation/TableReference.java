package com.gyeomgun.hbase.connector.annotation;

import com.gyeomgun.hbase.connector.entity.HbaseEntity;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created by hyungyeom on 19/01/2019.
 */
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface TableReference {
    Class<? extends HbaseEntity> value();
}
