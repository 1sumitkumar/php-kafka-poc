<?php

namespace App;
use RdKafka\Conf;
use RdKafka\Consumer;
use RdKafka\ConsumerTopic;
use RdKafka\Message;
class KafkaConsumer
{
    $conf = new RdKafka\Conf();
    $conf->set('log_level', (string) LOG_DEBUG);
    $conf->set('debug', 'all');
    $rk = new RdKafka\Consumer($conf);
    $rk->addBrokers("10.0.0.1,10.0.0.2");

    $topic = $rk->newTopic("test");

// The first argument is the partition to consume from.
// The second argument is the offset at which to start consumption. Valid values
// are: RD_KAFKA_OFFSET_BEGINNING, RD_KAFKA_OFFSET_END, RD_KAFKA_OFFSET_STORED.
$topic->consumeStart(0, RD_KAFKA_OFFSET_BEGINNING);

while (true) {
    // The first argument is the partition (again).
    // The second argument is the timeout.
    $msg = $topic->consume(0, 1000);
    if (null === $msg || $msg->err === RD_KAFKA_RESP_ERR__PARTITION_EOF) {
        // Constant check required by librdkafka 0.11.6. Newer librdkafka versions will return NULL instead.
        continue;
    } elseif ($msg->err) {
        echo $msg->errstr(), "\n";
        break;
    } else {
        echo $msg->payload, "\n";
    }
}
}
