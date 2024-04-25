<?php

namespace App\Http\Controllers;


use RdKafka\Conf;
// use RdKafka\Producer;
// use RdKafka\ProducerTopic;

class KafkaProducer
{

    public function produce()
    {
//        $conf = new \RdKafka\Conf();
//        $conf->set('log_level', (string) LOG_DEBUG);
//        $conf->set('metadata.broker.list', 'PLAINTEXT://broker:29092');
//        $producer = new \RdKafka\Producer($conf);
//        $topic = $producer->newTopic("topic_for_show");
//
//        for ($i = 0; $i < 5; $i++) {
//            $topic->produce(RD_KAFKA_PARTITION_UA, 0, "MENSAGE $i");
//            $producer->poll(0);
//        }
//
//        for ($flushRetries = 0; $flushRetries < 10; $flushRetries++) {
//            $result = $producer->flush(10000);
//            if (RD_KAFKA_RESP_ERR_NO_ERROR === $result) {
//                break;
//            }
//        }
//
//        if (RD_KAFKA_RESP_ERR_NO_ERROR !== $result) {
//            throw new \RuntimeException('Was unable to flush, messages might be lost!');
//        }
        echo "PUBLISHED";
    }



}

