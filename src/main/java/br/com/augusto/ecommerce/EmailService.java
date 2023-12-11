package br.com.augusto.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService {

    public static void main(String[] args) {
        var emailService = new EmailService();
        try(var service = new KafkaService(EmailService.class.getSimpleName(),
                "ECOMMERCE_SEND_EMAIL",
                emailService::parse,
                Email.class)){
        service.run();
        }
    }

    private void parse(ConsumerRecord<String, String> record){
        System.out.println("Sending email");
        System.out.println("Key = " + record.key());
        System.out.println("Value = " + record.value());
        System.out.println("Partition = " + record.partition());
        System.out.println("Record = " + record.offset());
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // ignoring
            e.printStackTrace();
        }
        System.out.println("Email Sent");
    }

}
