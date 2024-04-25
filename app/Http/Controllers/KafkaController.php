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
use Junges\Kafka\Message\KafkaAvroSchema;
use Junges\Kafka\Message\Registry\AvroSchemaRegistry;

use GuzzleHttp\Client;
class KafkaController extends Controller
{
    public function index()
    {
        //            Kafka::publishOn('test-topic')
        //                ->withKafkaKey(Str::uuid())
        //                ->withBodyKey('foo', 'bar')
        //                ->withHeaders([
        //                    'foo-header' => 'foo-value'
        //                ])
        //                ->send();
                echo 'I am the producer';
        $cachedRegistry = new CachedRegistry(
            new BlockingRegistry(
                new PromisingRegistry(
                    new Client(['base_uri' => 'schema-registry:8081'])
                )
            ),
            new AvroObjectCacheAdapter()
        );

        $registry = new AvroSchemaRegistry($cachedRegistry);
        $recordSerializer = new RecordSerializer($cachedRegistry);

        //if no version is defined, latest version will be used
        //if no schema definition is defined, the appropriate version will be fetched form the registry
        $registry->addBodySchemaMappingForTopic(
            'test-topic',
            new \Junges\Kafka\Message\KafkaAvroSchema('test_topic_value' /*, int $version, AvroSchema $definition */)
        );
        $registry->addKeySchemaMappingForTopic(
            'test-topic',
            new \Junges\Kafka\Message\KafkaAvroSchema('test_topic_value' /*, int $version, AvroSchema $definition */)
        );

        $serializer = new \Junges\Kafka\Message\Serializers\AvroSerializer($registry, $recordSerializer /*, AvroEncoderInterface::ENCODE_BODY */);

        //        $message = new Message(
        //            headers: ['header-key' => 'header-value'],
        //            body: ['key' => 'value'],
        //            key: 'kafka key here'
        //        );
       // $producer = \Junges\Kafka\Facades\Kafka::publishOn('test-topic')->withDebugEnabled()->withMessage($message)->usingSerializer($serializer);

        $message = new Message(
            body: [
                "id" =>  1,
                "statusId" => 3,
                "statusCode" =>  "RESOLVED"
            ]
        );


        //Kafka::publishOn('topic')->withMessage($message)->usingSerializer($serializer)->send();
        $producer = Kafka::publishOn('test-topic')->withMessage($message); #->usingSerializer($serializer);

        $producer->send();

        echo "PUBLISHED";
        die;
    }

    public function consume(){

        echo "Consuming";
        echo 'I am the consumer';
        \Log::info('This is some useful information.');
        Log::debug('This is just to test');
        //$consumer = Kafka::createConsumer(['test-topic'], 'group', 'broker:29092')->stopAfterLastMessage();
        //            ->withBrokers('broker:29092')
        //            ->withAutoCommit()
        //            ->withHandler(function(ConsumerMessage $message, MessageConsumer $consumer) {
        //                // Handle your message here
        //            })
        //            ->build();
        //
        //       $consumer->withHandler(function(ConsumerMessage $message, MessageConsumer $consumer) {
        //                // Handle your message here
        //           echo "<pre>";
        //           print_r($message);
        //           echo "</pre>";
        //            echo "dadsdsdsd";
        //            })->build()->consume();

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
        $registry->addBodySchemaMappingForTopic(
            'test-topic',
            new KafkaAvroSchema('test_topic_value' )
        );
        $consumer = Kafka::createConsumer(['test-topic'], 'group', 'broker:29092')->stopAfterLastMessage();
        $deserializer = new \Junges\Kafka\Message\Deserializers\AvroDeserializer($registry, $recordSerializer);
        $consumer = Kafka::createConsumer()
            ->subscribe('test-topic')
            ->withHandler(new TestHandler)
            ->withAutoCommit()
            ->build();

        $consumer->consume();

        die;
    }
}
