<?php /** @noinspection TypeUnsafeComparisonInspection */
/** @noinspection PhpUnhandledExceptionInspection */

declare(strict_types=1);

namespace Dnna\Enqueue\Axon;

use Interop\Queue\Consumer;
use Interop\Queue\Context;
use Interop\Queue\Destination;
use Interop\Queue\Exception\InvalidDestinationException;
use Interop\Queue\Exception\TemporaryQueueNotSupportedException;
use Interop\Queue\Message;
use Interop\Queue\Producer;
use Interop\Queue\Queue;
use Interop\Queue\SubscriptionConsumer;
use Interop\Queue\Topic;
use Io\Axoniq\Axonserver\Grpc\Command\CommandServiceClient;

class AxonContext implements Context
{
    use SerializerAwareTrait;

    /**
     * @var CommandServiceClient
     */
    private $axon;

    /**
     * @var callable
     */
    private $axonFactory;

    /**
     * @var int
     */
    private $redeliveryDelay;

    /**
     * Callable must return instance of Redis once called.
     *
     * @param CommandServiceClient|callable $axon
     * @param int $redeliveryDelay
     */
    public function __construct($axon, int $redeliveryDelay)
    {
        if ($axon instanceof CommandServiceClient) {
            $this->axon = $axon;
        } elseif (is_callable($axon)) {
            $this->axonFactory = $axon;
        } else {
            throw new \InvalidArgumentException(
                sprintf(
                    'The $axon argument must be either %s or callable that returns %s once called.',
                    CommandServiceClient::class,
                    CommandServiceClient::class
                )
            );
        }

        $this->redeliveryDelay = $redeliveryDelay;
        $this->setSerializer(new JsonSerializer());
    }

    /**
     * @param string $body
     * @param array $properties
     * @param array $headers
     * @return AxonMessage
     */
    public function createMessage(string $body = '', array $properties = [], array $headers = []): Message
    {
        return new AxonMessage($body, $properties, $headers);
    }

    /**
     * @param string $topicName
     * @return AxonDestination
     */
    public function createTopic(string $topicName): Topic
    {
        return new AxonDestination($topicName);
    }

    /**
     * @param string $queueName
     * @return AxonDestination
     */
    public function createQueue(string $queueName): Queue
    {
        return new AxonDestination($queueName);
    }

    /**
     * @param Queue $queue
     * @throws InvalidDestinationException
     */
    public function deleteQueue(Queue $queue): void
    {
        InvalidDestinationException::assertDestinationInstanceOf($queue, AxonDestination::class);

        throw new \DomainException('Not implemented');
    }

    /**
     * @param Topic $topic
     * @throws InvalidDestinationException
     */
    public function deleteTopic(Topic $topic): void
    {
        InvalidDestinationException::assertDestinationInstanceOf($topic, AxonDestination::class);

        throw new \DomainException('Not implemented');
    }

    public function createTemporaryQueue(): Queue
    {
        throw TemporaryQueueNotSupportedException::providerDoestNotSupportIt();
    }

    /**
     * @return Producer
     */
    public function createProducer(): Producer
    {
        return new AxonProducer($this);
    }

    /**
     * @param Destination $destination
     *
     * @return Consumer
     * @throws InvalidDestinationException
     */
    public function createConsumer(Destination $destination): Consumer
    {
        InvalidDestinationException::assertDestinationInstanceOf($destination, AxonDestination::class);

        $consumer = new AxonConsumer($this, $destination);
        $consumer->setRedeliveryDelay($this->redeliveryDelay);

        return $consumer;
    }

    /**
     * @return SubscriptionConsumer
     */
    public function createSubscriptionConsumer(): SubscriptionConsumer
    {
        $consumer = new AxonSubscriptionConsumer($this);
        $consumer->setRedeliveryDelay($this->redeliveryDelay);

        return $consumer;
    }

    /**
     * @param Queue $queue
     */
    public function purgeQueue(Queue $queue): void
    {
        throw new \DomainException('Not implemented');
    }

    public function close(): void
    {
        throw new \DomainException('Not implemented');
    }

    public function getAxon(): CommandServiceClient
    {
        if (false == $this->axon) {
            $axon = call_user_func($this->axonFactory);
            if (false == $axon instanceof CommandServiceClient) {
                throw new \LogicException(
                    sprintf(
                        'The factory must return instance of %s. It returned %s',
                        CommandServiceClient::class,
                        is_object($axon) ? get_class($axon) : gettype($axon)
                    )
                );
            }

            $this->axon = $axon;
        }

        return $this->axon;
    }
}
