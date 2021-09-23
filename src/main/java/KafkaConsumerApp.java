import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerApp {
  public static void main(String[] args){
    Properties props= new Properties();
    props.put("bootstrap.servers","localhost:9092");
    props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
    props.put("group.id","test");

    KafkaConsumer myConsumer=new KafkaConsumer(props);

   // we can read this list from db, dynamically? by fetching metadata?. there are ways to achieve this: https://stackoverflow.com/questions/36153783/kafka-consumer-to-dynamically-detect-topics-added
    //Also: The consumer supports a configuration option "metadata.max.age.ms" which basically controls how often topic metadata is fetched. By default, this is set fairly high (5 minutes), which means it will take up to 5 minutes to discover new topics matching your regular expression. You can set this lower to discover topics quicker.
    ArrayList<String> topics=new ArrayList<>();
    topics.add("my-topic");

    myConsumer.subscribe(topics);
    try{
      while(true){
        ConsumerRecords<String,String> records=myConsumer.poll(10);
        for(ConsumerRecord<String,String> record:records){
          System.out.println(String.format("Topic: %s, Partition: %d, offset: %d, Key: %s, Value: %s",
                                           record.topic(),record.partition(),record.offset(),record.key(),record.value()));

        }
      }

    }catch(Exception e) {
      System.out.println(e.getMessage());
    }finally {
      myConsumer.close();
    }
  }
}
