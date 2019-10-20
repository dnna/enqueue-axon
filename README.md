# enqueue-axon
Axon Transport for php-enqueue

## Installation
- `composer require dnna/enqueue-axon`

## Usage
- Before enqueue is initialized, you will need to register the `axon` scheme by adding the following 2 lines into your code:
`\Enqueue\Client\Resources::addDriver(AxonDriver::class, ['axon'], [], ['dnna/enqueue-axon']);
\Enqueue\Resources::addConnection(AxonConnectionFactory::class, ['axon'], [], 'dnna/enqueue-axon');`
