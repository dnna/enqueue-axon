<?php

declare(strict_types=1);

namespace Dnna\Enqueue\Axon;

use Interop\Queue\Consumer;
use Interop\Queue\SubscriptionConsumer;

class AxonSubscriptionConsumer implements SubscriptionConsumer
{
    /**
     * @var AxonContext
     */
    private $context;

    /**
     * an item contains an array: [AxonConsumer $consumer, callable $callback];.
     *
     * @var array
     */
    private $subscribers;

    /**
     * @var int
     */
    private $redeliveryDelay = 300;

    /**
     * @param AxonContext $context
     */
    public function __construct(AxonContext $context)
    {
        $this->context = $context;
        $this->subscribers = [];
    }

    /**
     * @return int
     */
    public function getRedeliveryDelay(): ?int
    {
        return $this->redeliveryDelay;
    }

    /**
     * @param int $delay
     */
    public function setRedeliveryDelay(int $delay): void
    {
        $this->redeliveryDelay = $delay;
    }

    public function consume(int $timeout = 0): void
    {
        if (empty($this->subscribers)) {
            throw new \LogicException('No subscribers');
        }

        $timeout = (int)ceil($timeout / 1000);
        $endAt = time() + $timeout;

        $queues = [];
        /** @var Consumer $consumer */
        foreach ($this->subscribers as list($consumer)) {
            $queues[] = $consumer->getQueue();
        }

        while (true) {
            throw new \Exception('TODO');
            if ($message = $this->receiveMessage($queues, $timeout ?: 5, $this->redeliveryDelay)) {
                list($consumer, $callback) = $this->subscribers[$message->getKey()];

                if (false === call_user_func($callback, $message, $consumer)) {
                    return;
                }
            }

            if ($timeout && microtime(true) >= $endAt) {
                return;
            }
        }
    }

    /**
     * @param AxonConsumer $consumer
     */
    public function subscribe(Consumer $consumer, callable $callback): void
    {
        if (false == $consumer instanceof AxonConsumer) {
            throw new \InvalidArgumentException(
                sprintf('The consumer must be instance of "%s" got "%s"', AxonConsumer::class, get_class($consumer))
            );
        }

        $queueName = $consumer->getQueue()->getQueueName();
        if (array_key_exists($queueName, $this->subscribers)) {
            if ($this->subscribers[$queueName][0] === $consumer && $this->subscribers[$queueName][1] === $callback) {
                return;
            }

            throw new \InvalidArgumentException(sprintf('There is a consumer subscribed to queue: "%s"', $queueName));
        }

        $this->subscribers[$queueName] = [$consumer, $callback];
        $this->queueNames = null;
    }

    /**
     * @param AxonConsumer $consumer
     */
    public function unsubscribe(Consumer $consumer): void
    {
        if (false == $consumer instanceof AxonConsumer) {
            throw new \InvalidArgumentException(
                sprintf('The consumer must be instance of "%s" got "%s"', AxonConsumer::class, get_class($consumer))
            );
        }

        $queueName = $consumer->getQueue()->getQueueName();

        if (false == array_key_exists($queueName, $this->subscribers)) {
            return;
        }

        if ($this->subscribers[$queueName][0] !== $consumer) {
            return;
        }

        unset($this->subscribers[$queueName]);
        $this->queueNames = null;
    }

    public function unsubscribeAll(): void
    {
        $this->subscribers = [];
        $this->queueNames = null;
    }

    private function getContext(): AxonContext
    {
        return $this->context;
    }
}
