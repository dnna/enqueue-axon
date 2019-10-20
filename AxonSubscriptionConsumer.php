<?php /** @noinspection PhpParamsInspection */

declare(strict_types=1);

namespace Dnna\Enqueue\Axon;

use Enqueue\Client\Config;
use Interop\Queue\Consumer;
use Interop\Queue\SubscriptionConsumer;
use Io\Axoniq\Axonserver\Grpc\Command\CommandProviderInbound;
use Io\Axoniq\Axonserver\Grpc\Command\CommandProviderOutbound;
use Io\Axoniq\Axonserver\Grpc\Command\CommandResponse;
use Io\Axoniq\Axonserver\Grpc\Command\CommandSubscription;
use Io\Axoniq\Axonserver\Grpc\FlowControl;
use Io\Axoniq\Axonserver\Grpc\SerializedObject;

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
            $queues[] = $consumer->getQueue()->getQueueName();
        }
        if (count($queues) > 1) {
            throw new \DomainException('TODO SUPPORT MULTIPLE QUEUES');
        }

        $stream = $this->context->openStream();
        $flowControl = new FlowControl();
        $flowControl->setPermits(2048);
        $commandProviderOutbound = new CommandProviderOutbound();
        $commandProviderOutbound->setFlowControl($flowControl);
        $stream->write($commandProviderOutbound);

        foreach ($this->context->getRouteCollection()->all() as $curRoute) {
            $commandSubscription = new CommandSubscription();
            $commandSubscription->setClientId($this->context->getConfig()->getApp() . '-c-' . $curRoute->getSource());
            $commandSubscription->setComponentName($this->context->getConfig()->getApp() . '-c-' . $curRoute->getSource());
            $commandSubscription->setCommand($curRoute->getSource());
            $commandProviderOutbound = new CommandProviderOutbound();
            $commandProviderOutbound->setSubscribe($commandSubscription);
            $stream->write($commandProviderOutbound);
        }
        while ($inboundCommand = $stream->read()) {
            /**
             * @var CommandProviderInbound $inboundCommand
             */
            /**
             * @var SerializedObject $payload
             */
            $payload = $inboundCommand->getCommand()->getPayload();
            $message = $this->getContext()->getSerializer()->toMessage($payload->getData());
            if ($message->getProperty(Config::COMMAND)) {
                if (!$message->getProperty('enqueue.processor')) {
                    /** @noinspection PhpStrictTypeCheckingInspection */
                    $route = $this->context->getRouteCollection()->command($inboundCommand->getCommand()->getName());
                    $message->setProperty('enqueue.processor', $route->getProcessor());
                }
            } else if ($message->getProperty(Config::TOPIC)) {
                throw new \DomainException('TODO SUPPORT TOPICS (NON-COMMANDS)');
            }

            //list($consumer, $callback) = $this->subscribers[$message->getKey()]; // This doesnt work because message->getKey is wrong
            list($consumer, $callback) = $this->subscribers[$queues[0]];
            if (false === call_user_func($callback, $message, $consumer)) {
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
