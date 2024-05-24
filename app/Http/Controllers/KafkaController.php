<?php

namespace App\Http\Controllers;

use Illuminate\Http\Request;
use RdKafka\Conf;
use Junges\Kafka\Facades\Kafka;
use Illuminate\Support\Facades\Log;
use Junges\Kafka\Contracts\KafkaConsumerMessage;
use Junges\Kafka\Message\ConsumedMessage;
use Junges\Kafka\Producers\MessageBatch;
use Junges\Kafka\Message\Message;
use App\Handlers\TestHandler;
use FlixTech\SchemaRegistryApi\Registry\Cache\AvroObjectCacheAdapter;
use FlixTech\SchemaRegistryApi\Registry\CachedRegistry;
use FlixTech\SchemaRegistryApi\Registry\PromisingRegistry;
use FlixTech\SchemaRegistryApi\Registry\BlockingRegistry;
use FlixTech\AvroSerializer\Objects\RecordSerializer;
use FlixTech\AvroSerializer\Objects\Schema;

use Junges\Kafka\Message\KafkaAvroSchema;
use Junges\Kafka\Message\Registry\AvroSchemaRegistry;
use Junges\Kafka\Contracts\KafkaAvroSchemaRegistry;
use Junges\Kafka\Handlers\RetryableHandler;
use Junges\Kafka\Handlers\RetryStrategies\DefaultRetryStrategy;
use Junges\Kafka\Commit\NativeSleeper;


use Junges\Kafka\Message\Serializers\AvroSerializer;
use Junges\Kafka\Message\Deserializers\AvroDeserializer;

use GuzzleHttp\Client;
class KafkaController extends Controller
{
    public function index()
    {
        echo 'I am the producer' . "\n\n" . "</br></br>";
        $cachedRegistry = new CachedRegistry(
            new BlockingRegistry(
                new PromisingRegistry(
                    new Client(['base_uri' => 'schema-registry:8081'])
                )
            ),
            new AvroObjectCacheAdapter()
        );

        $registry = new AvroSchemaRegistry($cachedRegistry);
        $recordSerializer = new RecordSerializer($cachedRegistry,  [
            // If you want to auto-register missing schemas set this to true
            RecordSerializer::OPTION_REGISTER_MISSING_SCHEMAS => true,
            // If you want to auto-register missing subjects set this to true
            RecordSerializer::OPTION_REGISTER_MISSING_SUBJECTS => true,
        ]);

        $topic = 'test-topic';
        $version = 1;

        $subject = $topic . '-value';

        $avroSchema = '{
          "type": "record",
          "name": "user",
          "fields": [
            {"name": "firstName", "type": "string"},
            {"name": "lastName", "type": "string"},
            {"name": "age", "type": "int"}
          ]
        }';

        echo "Avro Schema>>>>>: ";
        dump($avroSchema);

        $registry->addBodySchemaMappingForTopic(
            $topic,
            new \Junges\Kafka\Message\KafkaAvroSchema($subject, $version)
        );

        $serializer = new \Junges\Kafka\Message\Serializers\AvroSerializer($registry, $recordSerializer);
        $message = new Message(
            headers: ['some' => 'test header'],
            body: [
                'firstName' => 'John',
                'lastName' => 'Doe',
                'age' => 42,
            ]
        );
        echo "Body>>>>>>>:  ";
        dump( $message);
        $producer = \Junges\Kafka\Facades\Kafka::publish('broker:29092')->onTopic($topic)->withMessage($message)->usingSerializer($serializer);
        $producer->send();
        echo "PUBLISHED";
        die;
    }

    public function consume(){

        echo 'I am the consumer';
        \Log::info('This is some useful information.');
        Log::debug('This is just to test');
        $cachedRegistry = new CachedRegistry(
            new BlockingRegistry(
                new PromisingRegistry(
                    new Client(['base_uri' => 'schema-registry:8081'])
                )
            ),
            new AvroObjectCacheAdapter()
        );

        $registry = new \Junges\Kafka\Message\Registry\AvroSchemaRegistry($cachedRegistry);
        $recordSerializer = new RecordSerializer($cachedRegistry);
        $topic = 'test-topic';
        $version = 1;
        $registry->addBodySchemaMappingForTopic(
            'test-topic',
            new KafkaAvroSchema($topic.'-value', $version /*,AvroSchema $definition */ )
        );
        $registry->addKeySchemaMappingForTopic(
            'test-topic',
            new KafkaAvroSchema($topic.'-value', $version /*,AvroSchema $definition */)
        );
        $deserializer = new \Junges\Kafka\Message\Deserializers\AvroDeserializer($registry, $recordSerializer /*, AvroDecoderInterface::DECODE_BODY */);
        $consumer = Kafka::consumer(['test-topic'], 'group', 'broker:29092')->stopAfterLastMessage()
            ->withOptions([
                'auto.offset.reset' => 'earliest'
            ])
            ->usingDeserializer($deserializer)
            ->withHandler(new TestHandler)
            ->build();

        $consumer->consume();
        echo "Consuming" . "</br></br>";
        die;
    }
}
