package com.github.simplesteph.kafka.tutorial1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemoWithCallback {
    public static void main(String[] args) {

        final Logger loger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        String kafkaTopic = "firstTopic";
        String bootstrapServers = "127.0.0.1:9092";
        String message = "Hello Chicho!!";

        // Create producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i=0; i<10; i++) {


            //create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(kafkaTopic, message + Integer.toString(i));

            //send data asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //execute every time a record
                    if (e == null) {
                        loger.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp()
                        );
                    } else {
                        loger.error("Error while producing by", e);
                    }
                }
            });
        }

            //flush data
            producer.flush();

            //flush and close producer
            producer.close();

    }
}
