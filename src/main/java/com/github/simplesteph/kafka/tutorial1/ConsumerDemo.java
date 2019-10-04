package com.github.simplesteph.kafka.tutorial1;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Collections;


public class ConsumerDemo {
    public static void main(String[] args) {

        final Logger loger = LoggerFactory.getLogger(ConsumerDemo.class);

        String kafkaTopic = "firstTopic";
        String groupId = "my-group";
        String bootstrapServers = "127.0.0.1:9092";

        // Create consumer properties
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer kafka
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //subscriber consumer to our topic(s)
        consumer.subscribe(Arrays.asList(kafkaTopic));

        while(true){
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record:records){
                loger.info("Key: "+ record.key() + ", Value: " + record.value());
                loger.info("Partition: " + record.partition() + "Offset: "+ record.offset());

            }

        }

    }
}
