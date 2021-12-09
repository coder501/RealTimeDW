package com.atguigu.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkCDC02_SQL {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //动态表
        tableEnv.executeSql("CREATE TABLE user_info (" +
                " id INT ," +
                " name STRING," +
                " age INT," +
                ") WITH (" +
                " 'connector' = 'mysql-cdc'," +
                " 'hostname' = 'hadoop102'," +
                " 'port' = '3306'," +
                " 'username' = 'root'," +
                " 'password' = '123456'," +
                " 'database-name' = 'gmall_cdc'," +
                " 'table-name' = 'Student'" +
                ");");


        tableEnv.executeSql("select * from user_info").print();

        env.execute();
    }
}
