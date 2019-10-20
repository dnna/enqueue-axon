<?php

declare(strict_types=1);

namespace Dnna\Enqueue\Axon;

use Enqueue\Client\Config;
use Interop\Queue\Destination;
use Interop\Queue\Exception\InvalidDestinationException;
use Interop\Queue\Exception\InvalidMessageException;
use Interop\Queue\Exception\PriorityNotSupportedException;
use Interop\Queue\Message;
use Interop\Queue\Producer;
use Io\Axoniq\Axonserver\Grpc\Command\Command;
use Io\Axoniq\Axonserver\Grpc\Command\CommandResponse;
use Io\Axoniq\Axonserver\Grpc\SerializedObject;
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
        $message->setKey($destination->getName());

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
            //$deliveryAt = time() + $message->getDeliveryDelay();
            throw new InvalidMessageException('TODO SEND DELAYED MESSAGE');
        }

        if ($message->getProperty(Config::TOPIC)) {
            throw new InvalidMessageException('TODO HANDLE EVENTS (NON-COMMANDS)');
        }

        if (!$message->getReplyTo()) {
            throw new InvalidMessageException('TODO HANDLE COMMANDS THAT DO NOT REQUIRE REPLY');
        }

        if ($message->getProperty(Config::COMMAND)) {
            $command = new Command();
            $command->setClientId($this->context->getConfig()->getApp() . '-p-' . $message->getProperty(Config::COMMAND));
            $command->setComponentName($this->context->getConfig()->getApp() . '-p-' . $message->getProperty(Config::COMMAND));
            //$command->setMessageIdentifier($message->getMessageId());
            //$command->setTimestamp($message->getTimestamp());
            $command->setName($message->getProperty(Config::COMMAND));
            $srl = new SerializedObject();
            $srl->setData($payload);
            $command->setPayload($srl);

            /**
             * @var CommandResponse $reply
             */
            [$reply, $status] = $this->context->getAxon()->Dispatch($command)->wait();

            if (!$reply) {
                throw new InvalidDestinationException('Reply is null');
            }
            if ($reply->getErrorMessage() !== null) {
                throw new InvalidDestinationException($reply->getErrorMessage()->getMessage());
            }

            return;
        }

        throw new InvalidMessageException('Message is neither event nor command');
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
