//package com.gyeomgun.hbase.connector.repository;
//
//import com.gyeomgun.hbase.connector.index.IndexManager;
//import com.gyeomgun.hbase.connector.entity.Student;
//import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.Connection;
//import org.springframework.beans.factory.annotation.Qualifier;
//import org.springframework.stereotype.Repository;
//
//import java.io.IOException;
//import java.util.List;
//import java.util.Map;
//
///**
// * Created by hyungyeom on 19/01/2019.
// */
//@Repository
//public class StudentRepository extends BaseHbaseRepository<Student> {
//    public StudentRepository(@Qualifier("hbaseConnection") Connection connection,
//                             @Qualifier("indexManager") IndexManager indexManager) throws IOException {
//        super(connection.getTable(TableName.valueOf(Student.TABLE_NAME)), indexManager);
//        tableCreateIfNotExist(connection.getAdmin());
//    }
//
//    @Override
//    public Student getInstance(Map<String, List<String>> referenceMap) {
//        return new Student();
//    }
//
//}
