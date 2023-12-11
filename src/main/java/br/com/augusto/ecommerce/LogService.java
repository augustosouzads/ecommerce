package br.com.augusto.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.regex.Pattern;

public class LogService {

    public static void main(String[] args) {

        var logService = new LogService();
        try (var kafkaService = new KafkaService(LogService.class.getSimpleName(),
                Pattern.compile("ECOMMERCE.*"),
                logService::parse,
                String.class)){
            kafkaService.run();
        }
   }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("-------------- Message Received ------------------------------");
        System.out.println("TOPIC LOG = " + record.topic());
        System.out.println("key: " + record.key());
        System.out.println("value: " + record.value());
        System.out.println("partition: " + record.partition());
        System.out.println("offset: " + record.offset());
    }
}
