package com.gyeomgun.hbase.connector.config;

import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.test.autoconfigure.OverrideAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * Created by hyungyeom on 19/01/2019.
 */
@Configuration
@ComponentScan({ "com.gyeomgun.hbase.connector" })
@OverrideAutoConfiguration(enabled = false)
@Import(HbaseConnectorConfig.class)
@ImportAutoConfiguration(exclude = DataSourceAutoConfiguration.class)
public class SpringTestConfig {
}
