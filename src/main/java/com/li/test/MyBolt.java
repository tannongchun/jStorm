package com.li.test;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.esotericsoftware.minlog.Log;

import java.io.Serializable;
import java.util.Map;

/**
 * Title:
 * <p>
 * Description:TODO
 * <p>
 * Copyright:Copyright(c)2005
 * <p>
 * Company:
 * <p>
 * Author:lishuangjiang
 * <p>
 * Date:2018/4/12 10:10
 */
public class MyBolt implements IRichBolt,Serializable {

    private static final long serialVersionUID = 1L;

    OutputCollector collector;

    public void execute(Tuple input) {
        try {
            String string = input.getString(0);
            System.out.println(string+"................................");
            collector.ack(input);
        } catch (Exception e) {
            collector.fail(input);
            Log.error("解析数据异常", e);
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields());
    }

    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void cleanup() {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
