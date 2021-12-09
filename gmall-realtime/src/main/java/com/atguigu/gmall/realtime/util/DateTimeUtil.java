package com.atguigu.gmall.realtime.util;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DateTimeUtil {
    private static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    //将日期转为字符串
    public static String toYmdhms(Date date){
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());

        String dateStr = dtf.format(localDateTime);

        return dateStr;
    }

    //将字符串类型日期转换为时间毫秒数
    public static Long toTs(String dateStr){
        LocalDateTime localDateTime = LocalDateTime.parse(dateStr, dtf);

        Instant instant = localDateTime.toInstant(ZoneOffset.of("+8"));

        long ts = instant.toEpochMilli();

        return ts;
    }

    public static void main(String[] args) {
        System.out.println(ZoneId.systemDefault());
    }

}
