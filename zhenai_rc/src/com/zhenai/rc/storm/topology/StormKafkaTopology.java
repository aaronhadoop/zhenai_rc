package com.zhenai.rc.storm.topology;
              
import storm.kafka.BrokerHosts; 
import storm.kafka.KafkaSpout; 
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
                           
import com.google.common.collect.ImmutableList;
import com.zhenai.rc.storm.bolt.CounterBolt;
                  
public class StormKafkaTopology { 
                                
	public static void main(String[] args) {
		try{                                                       
//			String kafkaZookeeper = "rac1:2181,rac2:2181,rac3:2181"; 
			String kafkaZookeeper = "master:2181,slave1:2181,slave2:2181"; 
			BrokerHosts brokerHosts = new ZkHosts(kafkaZookeeper);         
			SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, "flume-kafka-storm-inte-1", "/flume-kafka-storm-inte-1", "id");
	        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());            
//	        kafkaConfig.zkServers =  ImmutableList.of("rac1","rac2","rac3");   
	        kafkaConfig.zkServers =  ImmutableList.of("master","slave1","slave2");  
	        kafkaConfig.zkPort = 2181;                                              
			                                                                                                
	        kafkaConfig.forceFromStart = true; 
			                
	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout("kafkaSpout", new KafkaSpout(kafkaConfig), 2);
	        builder.setBolt("counterBolt", new CounterBolt(),1).shuffleGrouping("kafkaSpout");
	                                                                                                                       
	        Config config = new Config();                         
	        config.setDebug(true);        
	                          
	        if(args!=null && args.length > 0) {              
	            config.setNumWorkers(2);  
	                                              
	            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
	        } else {                  
	            config.setMaxTaskParallelism(3);
	      
	            LocalCluster cluster = new LocalCluster();
	            cluster.submitTopology("special-topology", config, builder.createTopology());
	                                           
//	            Thread.sleep(500000);   
//	            cluster.shutdown(); 
	        }   
		}catch (Exception e) {
			e.printStackTrace();
		}
	}

} 
