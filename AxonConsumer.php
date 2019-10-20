<?php

declare(strict_types=1);

namespace Dnna\Enqueue\Axon;

use Interop\Queue\Consumer;
use Interop\Queue\Exception\InvalidMessageException;
use Interop\Queue\Message;
use Interop\Queue\Queue;
use Io\Axoniq\Axonserver\Grpc\Command\CommandServiceClient;

class AxonConsumer implements Consumer
{
    /**
     * @var AxonDestination
     */
    private $queue;

    /**
     * @var AxonContext
     */
    private $context;

    /**
     * @var int
     */
    private $redeliveryDelay = 300;

    public function __construct(AxonContext $context, AxonDestination $queue)
    {
        $this->context = $context;
        $this->queue = $queue;
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

    /**
     * @return AxonDestination
     */
    public function getQueue(): Queue
    {
        return $this->queue;
    }

    /**
     * @return AxonMessage
     */
    public function receive(int $timeout = 0): ?Message
    {
        $timeout = (int) ceil($timeout / 1000);

        if ($timeout <= 0) {
            while (true) {
                if ($message = $this->receive(5000)) {
                    return $message;
                }
            }
        }

        throw new \Exception('TODO');
        //return $this->receiveMessage([$this->queue], $timeout, $this->redeliveryDelay);
    }

    /**
     * @return AxonMessage
     */
    public function receiveNoWait(): ?Message
    {
        throw new \Exception('TODO');
        //return $this->receiveMessageNoWait($this->queue, $this->redeliveryDelay);
    }

    /**
     * @param AxonMessage $message
     */
    public function acknowledge(Message $message): void
    {
        throw new \Exception('TODO');
        //$this->getAxon()->zrem($this->queue->getName().':reserved', $message->getReservedKey());
    }

    /**
     * @param AxonMessage $message
     * @throws InvalidMessageException
     */
    public function reject(Message $message, bool $requeue = false): void
    {
        InvalidMessageException::assertMessageInstanceOf($message, AxonMessage::class);

        $this->acknowledge($message);

        if ($requeue) {
            $message = $this->getContext()->getSerializer()->toMessage($message->getReservedKey());
            $message->setHeader('attempts', 0);

            if ($message->getTimeToLive()) {
                $message->setHeader('expires_at', time() + $message->getTimeToLive());
            }

            $payload = $this->getContext()->getSerializer()->toString($message);

            throw new \Exception('TODO');
            //$this->getAxon()->lpush($this->queue->getName(), $payload);
        }
    }

    private function getContext(): AxonContext
    {
        return $this->context;
    }

    private function getAxon(): CommandServiceClient
    {
        return $this->context->getAxon();
    }
}
