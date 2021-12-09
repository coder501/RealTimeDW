package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * PatternProcessFunction有一个processMatch的方法在每找到一个匹配的事件序列时都会被调用
 * 当一个模式上通过within加上窗口长度后，部分匹配的事件序列就可能因为超过窗口长度而被丢弃。
 * 可以使用TimedOutPartialMatchHandler接口 来处理超时的部分匹配。这个接口可以和其它的混合使用。
 * 也就是说你可以在自己的PatternProcessFunction里另外实现这个接口。
 * TimedOutPartialMatchHandler提供了另外的processTimedOutMatch方法，这个方法对每个超时的部分匹配都会调用。
 */

public class MyPatternProcessFunction extends PatternProcessFunction<JSONObject, String> implements TimedOutPartialMatchHandler<JSONObject> {

    private OutputTag<String> outputTag;

    public MyPatternProcessFunction(OutputTag<String> outputTag) {
        this.outputTag = outputTag;
    }

    @Override
    public void processMatch(Map<String, List<JSONObject>> match, PatternProcessFunction.Context ctx, Collector<String> out) throws Exception {

    }

    /**
     *
     * @param match
     * @param ctx
     * @throws Exception
     *  注： processTimedOutMatch不能访问主输出。 但你可以通过Context对象把结果输出到侧输出
     */
    @Override
    public void processTimedOutMatch(Map<String, List<JSONObject>> match, Context ctx) throws Exception {
        List<JSONObject> timeOutJsonObj = match.get("first");
        for (JSONObject jsonObject : timeOutJsonObj) {
            ctx.output(outputTag, jsonObject.toJSONString());
        }
    }
}