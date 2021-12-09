package com.atguigu.gmall.realtime.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

public class DimUtil {




    /*
    添加redis作为旁路缓存
    redis数据类型     type:String   key:dim:维度表名:主键值1_主键值2  ttl:1day
     */
    public static JSONObject getDimInfo(String tableName,Tuple2<String,String>... columnNameAndValues){

        StringBuilder querySql = new StringBuilder("select * from "+ tableName + " where ");

        StringBuilder redisKey = new StringBuilder("dim:"+tableName.toLowerCase()+":");

        for (int i = 0; i < columnNameAndValues.length; i++) {
            Tuple2<String, String> columnNameAndValue = columnNameAndValues[i];
            String columnName = columnNameAndValue.f0;
            String columnValue = columnNameAndValue.f1;

            querySql.append(columnName).append("='").append(columnValue).append("'");
            //redisKey.append(columnName);
            //redis中是主键值而不是主键的列名
            redisKey.append(columnValue);

            if (i != columnNameAndValues.length - 1) {
                querySql.append(" and ");
                redisKey.append("_");
            }
        }

            Jedis jedis = null;
            //用于存储从redis中查出来的jsonString
            String jsonStr = null;
            //最终的查询结果
            JSONObject dimJsonObj = null;

            try {
                jedis = RedisUtil.getJedis();
                jsonStr =jedis.get(redisKey.toString());
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("从redis中获取数据失败");
            }

            if(jsonStr != null && jsonStr.length()>0){
                dimJsonObj = JSON.parseObject(jsonStr);
            }else {
                //System.out.println("从phoenix中查询数据的sql "+querySql);
                List<JSONObject> jsonObjList = PhoenixUtil.queryList(querySql.toString(), JSONObject.class);
                if(jsonObjList != null && jsonObjList.size() > 0){
                    dimJsonObj = jsonObjList.get(0);

                    //将查询到的数据缓存到redis中
                    if(jedis != null){
                        jedis.setex(redisKey.toString(),3600*24,dimJsonObj.toJSONString());
                    }
                }else{
                    System.out.println("从phoenix表中获取数据失败");
                }
            }
            //释放资源
        if(jedis != null){
            jedis.close();
        }


        return dimJsonObj;
    }

    //TODO 字段名称必须是大写 因为在phoenix中表名和字段名都是以大写形式存在的
    public static JSONObject getDimInfo(String tableName,String id){
       return getDimInfo(tableName,Tuple2.of("ID",id));
    }




    //select * from dim_user_info where id='1';
    public static JSONObject getDimInfoNoCache(String tableName, Tuple2<String,String>... columnNameAndValue) {

        JSONObject ResJsonObj = new JSONObject();

        StringBuilder querySql = new StringBuilder("select * from" + tableName + "where ");
        for(int i=0;i<columnNameAndValue.length;i++){
            Tuple2<String, String> condition = columnNameAndValue[i];
            String columnName = condition.f0;
            String columnValue = condition.f1;
            querySql.append(columnName +"='"+columnValue+"'");
            if(i != columnNameAndValue.length-1){
                querySql.append(" and ");
            }
        }
        System.out.println("查询的sql为：" + querySql);

        //调用PhoenixUtil工具类查询结果集
        List<JSONObject> resObjList = PhoenixUtil.queryList(querySql.toString(), JSONObject.class);

        if(resObjList != null && resObjList.size()>0){
            ResJsonObj = resObjList.get(0);
        }else {
            System.out.println("维度数据未找到");
        }




        return ResJsonObj;
    }

    public static void deleteCached(String tableName,String id){
        try {
            String redisKey = "dim:" + tableName.toLowerCase() + ":" + id;
            Jedis jedis = RedisUtil.getJedis();
            jedis.del(redisKey);
            jedis.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("从redis中删除数据发生了异常");
        }
    }


    public static void main(String[] args) {
        JSONObject dimInfo = DimUtil.getDimInfo("DIM_USER_INFO", "5");
        System.out.println(dimInfo);
    }

}
