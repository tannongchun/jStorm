package com.li.test;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

import java.util.ArrayList;
import java.util.List;

/**
 * Title:
 * <p>
 * Description:TODO
 * <p>
 * Copyright:Copyright(c)2005
 * <p>
 * Company:skycomm.com.cn
 * <p>
 * Author:lishuangjiang
 * <p>
 * Date:2018/4/12 10:00
 */
public class MyTopology {

    public static void main(String[] args) throws InterruptedException {

        String brokerZkStr = "119.23.20.*:2181,120.77.200.*:2181,39.108.5.*:2181";
        String brokerZkPath = "/brokers";
        //消费kafka得top
        String topic = "testTopic";
        String offset = "";
        //id 可以随意命名
        String id = "testTopic";
        Integer workerNumSpout = 3;
        Integer workerNumBolt = 3;
        Integer maxSpoutPending = 2000;

        if(args.length > 1){
            topic = args[1];
        }
        if(args.length > 2){
            workerNumSpout = Integer.parseInt(args[2]);
            workerNumBolt = Integer.parseInt(args[3]);
        }
        if(args.length > 4){
            maxSpoutPending = Integer.parseInt(args[4]);
        }
        ZkHosts zk = new ZkHosts(brokerZkStr,brokerZkPath);
        SpoutConfig spoutConf = new SpoutConfig(zk, topic,
                offset,
                id);

        List<String> zkServices = new ArrayList<String>();

        for(String str : zk.brokerZkStr.split(",")){
            zkServices.add(str.split(":")[0]);
        }

        spoutConf.zkServers = zkServices;
        spoutConf.zkPort = 2181;
        spoutConf.forceFromStart = false;
        spoutConf.socketTimeoutMs = 60 * 1000;
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());

        TopologyBuilder builder = new TopologyBuilder();
        // Kafka我们创建了一个5分区的Topic，这里并行度设置为5
        builder.setSpout("data", new KafkaSpout(spoutConf), workerNumSpout);
        builder.setBolt("analyze", new MyBolt(), workerNumBolt) .shuffleGrouping("data");

        Config config = new Config();
        config.setDebug(false);
        config.setNumWorkers(workerNumSpout);
        config.setMaxSpoutPending(1);
        config.setNumAckers(0);
        config.setDebug(false);
        if(maxSpoutPending > 0){
            config.setMaxSpoutPending(maxSpoutPending);
        }
        System.out.println(" topic = " + topic + " workerNumSpout = " + workerNumSpout +
                " workerNumBolt = " + workerNumBolt + " maxSpoutPending = " + maxSpoutPending);

        if(args.length>0){
            try {
                // args有参数时在分布式上提交任务
                StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }else{
            // args没有参数时在本地提交任务
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("ImsiTopology", config, builder.createTopology());
        }
    }
}
