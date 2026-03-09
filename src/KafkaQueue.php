<?php
namespace Kafka;

use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;

class KafkaQueue extends Queue implements QueueContract
{
    protected $consumer, $producer;

    public function __construct($producer, $consumer)
    {

        $this->producer = $producer;
        $this->consumer = $consumer;
    }

    public function size($queue = null)
    {}

    public function push($job, $data = '', $queue = null)
    {
        $topic = $this->producer->newTopic($queue ?? env('KAFKA_QUEUE'));

        $topic->produce(RD_KAFKA_PARTITION_UA, 0, serialize($job));
        // $topic->produce(RD_KAFKA_PARTITION_UA, 0, json_encode($job));
        // $topic->produce(RD_KAFKA_PARTITION_UA, 0, "Hello From the Ambassador APP");

        $this->producer->flush(100000);

        // $this->producer->poll(0);

        // for ($retries = 0; $retries < 10; $retries++) {
        //     $result = $this->producer->flush(1000);
        //     if ($result === RD_KAFKA_RESP_ERR_NO_ERROR) {
        //         return true;
        //     }
        // }

        // throw new \RuntimeException('Kafka producer flush failed');

    }

    public function pushRaw($payload, $queue = null, array $options = [])
    {}

    public function later($delay, $job, $data = '', $queue = null)
    {}

    public function pop($queue = null)
    {
        // Subscribe to the topic (use env default if $queue not provided)
        $this->consumer->subscribe([$queue ?? env('KAFKA_QUEUE')]);

        try {
            // Consume a single message (timeout in ms)
            $message = $this->consumer->consume(120 * 1000);

            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    // Unserialize the job object
                    $job = unserialize($message->payload);
                    $job->handle();
                    // $job = json_decode($message->payload, true);

                    // if ($job) {
                    //     // Run the job immediately
                    //     var_dump($job);
                    //     // $job->handle();
                    //     echo "Job executed successfully\n";
                    // } else {
                    //     var_dump("Failed to unserialize job payload:", $message->payload);
                    // }
                    // var_dump($message->payload);
                    // return $message->payload;
                    // return $job;
                    break;

                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    var_dump("No more messages");
                    return null;

                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    var_dump("Timed Out");
                    return null;

                default:
                    throw new \Exception($message->errstr(), $message->err);
            }

        } catch (\Exception $e) {
            var_dump("Kafka consume exception:", $e->getMessage());
            return null;
        }
    }

    public function __call(string $name, array $arguments)
    {}
}
