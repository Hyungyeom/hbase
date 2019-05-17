package com.gyeomgun.hbase.connector.join.type;

import java.lang.reflect.Field;

import com.gyeomgun.hbase.connector.context.Context;
import com.gyeomgun.hbase.connector.index.IndexManager;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Builder
public class JoinTypeProcessorParameter {
    private String fromKey;
    private String fromAlias;
    private Field fromField;
    private String joinKey;
    private String joinAlias;
    private Field joinField;
    private Field referenceField;
    private Context joinContext;
    private IndexManager indexManager;
}
