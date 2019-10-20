<?php /** @noinspection NullPointerExceptionInspection */

declare(strict_types=1);

namespace Dnna\Enqueue\Axon;

use Enqueue\Client\Config;
use Enqueue\Client\Driver\GenericDriver;
use Enqueue\Client\DriverSendResult;
use Enqueue\Client\Message;
use Enqueue\Client\Route;
use Enqueue\Client\RouteCollection;
use Interop\Queue\Queue as InteropQueue;

/**
 * @method AxonContext getContext
 */
class AxonDriver extends GenericDriver
{
    private $context;

    public function __construct(AxonContext $context, ...$args)
    {
        if (!$args[0] instanceof Config) {
            throw new \DomainException('Argument #1 should be a config');
        }
        if (!$args[1] instanceof RouteCollection) {
            throw new \DomainException('Argument #1 should be a route collection');
        }
        $this->context = $context;
        $this->context->setConfig($args[0]);
        $this->context->setRouteCollection($args[1]);
        parent::__construct($context, ...$args);
    }

    // We have to override this whole method here to prevent 'There is no route for command "%s".' errors
    public function sendToProcessor(Message $message): DriverSendResult
    {
        $topic = $message->getProperty(Config::TOPIC);
        $command = $message->getProperty(Config::COMMAND);

        $transportMessage = $this->createTransportMessage($message);

        $producer = $this->context->createProducer();

        if (null !== $delay = $transportMessage->getProperty(Config::DELAY)) {
            $producer->setDeliveryDelay($delay * 1000);
        }

        if (null !== $expire = $transportMessage->getProperty(Config::EXPIRE)) {
            $producer->setTimeToLive($expire * 1000);
        }

        if (null !== $priority = $transportMessage->getProperty(Config::PRIORITY)) {
            $priorityMap = $this->getPriorityMap();

            $producer->setPriority($priorityMap[$priority]);
        }

        $queue = $this->context->createQueue($topic ?? $command);

        $this->doSendToProcessor($producer, $queue, $transportMessage);

        return new DriverSendResult($queue, $transportMessage);
    }
}
