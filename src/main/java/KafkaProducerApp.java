import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaProducerApp {
  public static void main(String[] args){
    Properties props= new Properties();
    props.put("bootstrap.servers","localhost:9092");
    props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

    KafkaProducer myProducer= new KafkaProducer(props);
    ProducerRecord myRecord=new ProducerRecord("my_topic","Course-001","My Message 1");
    myProducer.send(myRecord);

    try{
      for(int i=0;i<1500000;i++){
        myProducer.send(new ProducerRecord<String ,String>("my-topic", Integer.toString(i),"my message"+Integer.toString(i)));
        TimeUnit.SECONDS.sleep(1);

      }
    }catch (Exception e){
      e.printStackTrace();;
    }finally {
      myProducer.close();
    }
  }
}
