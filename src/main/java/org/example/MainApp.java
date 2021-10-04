package org.example;

import java.util.ArrayList;

public class MainApp {

  public static void main(String[] args){

    final String brokers="localhost:9092";

    ProducerThread producer=new ProducerThread(brokers,"my-topic");
    Thread t1 = new Thread(producer);
    t1.start();

    ProducerThread anotherProducer=new ProducerThread(brokers,"my-other-topic");
    Thread t2 = new Thread(anotherProducer);
    t2.start();

    ArrayList<String> topics1=new ArrayList<>();
    topics1.add("my-topic");
    ConsumerThread consumer=new ConsumerThread(brokers,topics1,"test1");
    Thread t3 = new Thread(consumer);
    t3.start();


    ArrayList<String> topics2=new ArrayList<>();
    topics1.add("my-other-topic");
    ConsumerThread anotherConsumer=new ConsumerThread("localhost:9092",topics2,"test2");
    Thread t4 = new Thread(anotherConsumer);
    t4.start();
  }
}
