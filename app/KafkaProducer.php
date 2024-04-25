<?php

namespace App;

use RdKafka\Producer;
use RdKafka\ProducerTopic;

class KafkaProducer
{
    $conf = new RdKafka\Conf();
    $conf->set('log_level', (string) LOG_DEBUG);
    $conf->set('debug', 'all');
    $rk = new RdKafka\Producer($conf);
    $rk->addBrokers("10.0.0.1:9092,10.0.0.2:9092");
    $topic = $rk->newTopic("test");

    $topic->produce(RD_KAFKA_PARTITION_UA, 0, "Message payload");

    
}
