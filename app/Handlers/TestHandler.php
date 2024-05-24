<?php

namespace App\Handlers;

use Illuminate\Support\Facades\Log;
use Junges\Kafka\Contracts\ConsumerMessage;
use Junges\Kafka\Contracts\MessageConsumer;

class TestHandler
{
    public function __invoke(\Junges\Kafka\Contracts\ConsumerMessage $message, \Junges\Kafka\Contracts\MessageConsumer $consumer) {
        // Handle your message here
        Log::debug('Message received!', [
            'body' => $message->getBody(),
            'headers' => $message->getHeaders(),
            'partition' => $message->getPartition(),
            'key' => $message->getKey(),
            'topic' => $message->getTopicName()
        ]);
    }
}
