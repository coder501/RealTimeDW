package com.atguigu.gmall.realtime.util;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/*
    线程池工具类
 */
public class ThreadPoolUtil {
    public static ThreadPoolExecutor pool;

    public static ThreadPoolExecutor getInstance(){
        if(pool == null){
            synchronized (ThreadPoolUtil.class){
                if(pool == null){
                    pool = new ThreadPoolExecutor(4,20,300, TimeUnit.SECONDS,
                            new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE));
                }
            }
        }

        return pool;
    }
}
