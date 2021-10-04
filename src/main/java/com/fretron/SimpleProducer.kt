package com.fretron

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.*

class SimpleProducer {

}

fun main() {

    val scanner = Scanner(System.`in`)
   print("Enter topic name : ")
    val topicName = scanner.nextLine()

    val producer = KafkaProducer<String, String>(getProperties())
    var i:Long=1
    while(true) {
        println(i)
        val producerRecord = ProducerRecord(topicName, i.toString(), i.toString())
        producer.send(producerRecord)
        ++i
              // Thread.sleep(500)
    }

    ("Message sent successfully")

    producer.close()


}

fun getProperties():Properties{
    // create instance for properties to access producer configs
    val props = Properties()

    //Assign localhost id
    props.put("bootstrap.servers", "localhost:9092")

    //Set acknowledgements for producer requests.
    props.put("acks", "all")

    //If the request fails, the producer can automatically retry,
    props.put("retries", 0)

    //Specify buffer size in config
    props.put("batch.size", 16384)

    //time to wait before sending msgs to Kafka
    props.put("linger.ms", 0)

    //The buffer.memory - the total amount of memory available to the producer for buffering.
    props.put("buffer.memory", 33554432)

    props.put(
        "key.serializer",
        "org.apache.kafka.common.serialization.StringSerializer"
    )

    props.put(
        "value.serializer",
        "org.apache.kafka.common.serialization.StringSerializer"
    )

    return props
}

