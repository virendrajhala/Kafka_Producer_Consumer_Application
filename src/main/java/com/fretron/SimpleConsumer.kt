package com.fretron

import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.*

class SimpleConsumer {


}

fun main(){

    val scanner = Scanner(System.`in`)
    print("Enter topic name : ")
    val topicName = scanner.nextLine()


    val consumer = KafkaConsumer<String,String>(getConsumerProperties())

    //Kafka Consumer subscribes to  a list of topics here.
    consumer.subscribe(Arrays.asList(topicName))

    //print the topic name
    println("Subscribed to topic $topicName")

    while (true){
        val consumerRecords = consumer.poll(100)

        for(record in consumerRecords){

            Thread.sleep(15000)
            // print the offset,key and value for the consumer records.
            println("Offset : ${record.offset()},   Key : ${record.key()},  Value : ${record.value()}")


        }
    }
}

fun getConsumerProperties():Properties{
    //Kafka Consumer Config setting
    val props = Properties()

    props.put("bootstrap.servers","localhost:9092")

    props.put("group.id", "test")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")
    props.put("key.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer")

    return props
}