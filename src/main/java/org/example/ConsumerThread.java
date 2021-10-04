package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Properties;

public class ConsumerThread implements Runnable{

  private final KafkaConsumer<String,String> consumer;
  private final ArrayList<String> topics;

  public ConsumerThread(String brokers, ArrayList<String> topics,String groupId) {
    this.consumer = new KafkaConsumer<String,String>(createConsumerConfig(brokers,groupId));
    this.topics=topics;
  }

  private static Properties createConsumerConfig(String brokers, String groupId) {
    Properties props = new Properties();
    props.put("bootstrap.servers", brokers);
    props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
    props.put("group.id",groupId);
    return props;
  }


  @Override
  public void run() {
    consumer.subscribe(topics);
    try{
      while(true){
        ConsumerRecords<String,String> records=consumer.poll(10);
        for(ConsumerRecord<String,String> consumedRecord:records){
          System.out.println(String.format("Consumed message:  Topic: %s, Partition: %d, offset: %d, Key: %s, Value: %s",
            consumedRecord.topic(),consumedRecord.partition(),consumedRecord.offset(),consumedRecord.key(),consumedRecord.value()));
        }
      }

    }catch(Exception e) {
      System.out.println(e.getMessage());
    }finally {
      consumer.close();
    }
  }
}
