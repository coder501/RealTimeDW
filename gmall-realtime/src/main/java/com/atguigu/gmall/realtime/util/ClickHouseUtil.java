package com.atguigu.gmall.realtime.util;

import com.atguigu.gmall.realtime.bean.TransientSink;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHouseUtil {
    public static <T> SinkFunction<T> getJdbcsink(String sql) {
        SinkFunction sink = JdbcSink.sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T obj) throws SQLException {
                        Field[] fieldArr = obj.getClass().getDeclaredFields();
                        //用来标记不需要写到clickhouse的字段的个数
                        int skipNum = 0;

                        for(int i = 0;i<fieldArr.length;i++){

                            Field field = fieldArr[i];
                            TransientSink annotation = field.getAnnotation(TransientSink.class);
                            if(annotation != null){
                                skipNum++;
                                continue;
                            }
                            //是需要往clickhouse写入的字段
                            field.setAccessible(true);
                            try {
                                Object fieldValue = field.get(obj);
                                preparedStatement.setObject(i + 1 - skipNum,fieldValue);

                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }


                        }
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchIntervalMs(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                .withUrl(GmallConfig.CLICKHOUSE_DRIVER)
                .build()
        );

        return sink;
    }
}
