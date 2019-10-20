<?php

declare(strict_types=1);

namespace Dnna\Enqueue\Axon;

use Interop\Queue\Destination;
use Interop\Queue\Exception\InvalidDestinationException;
use Interop\Queue\Exception\InvalidMessageException;
use Interop\Queue\Exception\PriorityNotSupportedException;
use Interop\Queue\Message;
use Interop\Queue\Producer;
use Ramsey\Uuid\Uuid;

class AxonProducer implements Producer
{
    /**
     * @var AxonContext
     */
    private $context;

    /**
     * @var int|null
     */
    private $timeToLive;

    /**
     * @var int
     */
    private $deliveryDelay;

    /**
     * @param AxonContext $context
     */
    public function __construct(AxonContext $context)
    {
        $this->context = $context;
    }

    /**
     * @param Destination $destination
     * @param Message $message
     * @throws InvalidDestinationException
     * @throws InvalidMessageException
     * @throws \Exception
     */
    public function send(Destination $destination, Message $message): void
    {
        /** @var AxonDestination $destination */
        /** @var AxonMessage $message */
        InvalidDestinationException::assertDestinationInstanceOf($destination, AxonDestination::class);
        InvalidMessageException::assertMessageInstanceOf($message, AxonMessage::class);

        $message->setMessageId(Uuid::uuid4()->toString());
        $message->setHeader('attempts', 0);

        if (null !== $this->timeToLive && null === $message->getTimeToLive()) {
            $message->setTimeToLive($this->timeToLive);
        }

        if (null !== $this->deliveryDelay && null === $message->getDeliveryDelay()) {
            $message->setDeliveryDelay($this->deliveryDelay);
        }

        if ($message->getTimeToLive()) {
            $message->setHeader('expires_at', time() + $message->getTimeToLive());
        }

        $payload = $this->context->getSerializer()->toString($message);

        if ($message->getDeliveryDelay()) {
            $deliveryAt = time() + $message->getDeliveryDelay();
            throw new \Exception('TODO SEND MESSAGE HERE');
            //$this->context->getRedis()->zadd($destination->getName().':delayed', $payload, $deliveryAt);
        } else {
            throw new \Exception('TODO SEND MESSAGE HERE');
            //$this->context->getRedis()->lpush($destination->getName(), $payload);
        }
    }

    /**
     * @param int|null $deliveryDelay
     * @return self
     */
    public function setDeliveryDelay(int $deliveryDelay = null): Producer
    {
        $this->deliveryDelay = $deliveryDelay;

        return $this;
    }

    public function getDeliveryDelay(): ?int
    {
        return $this->deliveryDelay;
    }

    /**
     * @param int|null $priority
     * @return AxonProducer
     * @throws PriorityNotSupportedException
     */
    public function setPriority(int $priority = null): Producer
    {
        if (null === $priority) {
            return $this;
        }

        throw PriorityNotSupportedException::providerDoestNotSupportIt();
    }

    public function getPriority(): ?int
    {
        return null;
    }

    /**
     * @param int|null $timeToLive
     * @return self
     */
    public function setTimeToLive(int $timeToLive = null): Producer
    {
        $this->timeToLive = $timeToLive;

        return $this;
    }

    public function getTimeToLive(): ?int
    {
        return $this->timeToLive;
    }
}
