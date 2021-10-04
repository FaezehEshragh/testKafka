package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ProducerThread implements  Runnable{

  private final KafkaProducer<String, String> producer;
  private final String topic;

  public ProducerThread(String brokers, String topic) {
    Properties prop = createProducerConfig(brokers);
    this.producer = new KafkaProducer<String, String>(prop);
    this.topic = topic;
  }

  private static Properties createProducerConfig(String brokers) {
    Properties props = new Properties();
    props.put("bootstrap.servers", brokers);
     props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    return props;
  }

  @Override
  public void run() {
    try{
      for(int i=0;i<150;i++){
        producer.send(
          new ProducerRecord<String ,String>(topic,
            Integer.toString(i),"my message"+Integer.toString(i)));
        System.out.println(String.format("sent message %d to topic %s",i, topic));
        TimeUnit.SECONDS.sleep(1);

      }
    }catch (Exception e){
      e.printStackTrace();;
    }finally {
      producer.close();
    }
  }
}
