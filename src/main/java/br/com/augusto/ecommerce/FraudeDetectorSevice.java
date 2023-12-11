package br.com.augusto.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudeDetectorSevice {

    public static void main(String[] args) {

        var fraudeService = new FraudeDetectorSevice();
        try (var service = new KafkaService<>(FraudeDetectorSevice.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER" ,
                fraudeService::parse,
                Order.class)) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Order> record) {
            System.out.println("Processing new order, checking for fraud");
            System.out.println("key: " + record.key());
            System.out.println("value: " + record.value());
            System.out.println("partition: " + record.partition());
            System.out.println("offset: " + record.offset());
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // ignoring
                e.printStackTrace();
            }
            System.out.println("Order processed");
    }
}
