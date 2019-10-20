<?php /** @noinspection TypeUnsafeComparisonInspection */
/** @noinspection PhpComposerExtensionStubsInspection */
/** @noinspection CallableParameterUseCaseInTypeContextInspection */

declare(strict_types=1);

namespace Dnna\Enqueue\Axon;

use Enqueue\Dsn\Dsn;
use Grpc\ChannelCredentials;
use Interop\Queue\ConnectionFactory;
use Interop\Queue\Context;
use Io\Axoniq\Axonserver\Grpc\Command\CommandServiceClient;

class AxonConnectionFactory implements ConnectionFactory
{
    /**
     * @var array
     */
    private $config;

    /**
     * @var CommandServiceClient
     */
    private $axon;

    /**
     * axon://h:asdfqwer1234asdf@ec2-111-1-1-1.compute-1.amazonaws.com:8124
     *
     * or
     *
     * instance of Io\Axoniq\Axonserver\Grpc\Command\CommandServiceClient;
     *
     * @param array|string|CommandServiceClient|null $config
     */
    public function __construct($config = 'axon:')
    {
        if ($config instanceof CommandServiceClient) {
            $this->axon = $config;
            $this->config = $this->defaultConfig();

            return;
        }

        if (empty($config)) {
            $config = [];
        } elseif (is_string($config)) {
            $config = $this->parseDsn($config);
        } elseif (is_array($config)) {
            if (array_key_exists('dsn', $config)) {
                $config = array_replace_recursive($config, $this->parseDsn($config['dsn']));

                unset($config['dsn']);
            }
        } else {
            throw new \LogicException(
                sprintf(
                    'The config must be either an array of options, a DSN string, null or instance of %s',
                    CommandServiceClient::class
                )
            );
        }

        $this->config = array_replace($this->defaultConfig(), $config);
    }

    /**
     * @return AxonContext
     */
    public function createContext(): Context
    {
        if ($this->config['lazy']) {
            return new AxonContext(
                function () {
                    return $this->createAxon();
                }, $this->config['redelivery_delay']
            );
        }

        return new AxonContext($this->createAxon(), $this->config['redelivery_delay']);
    }

    private function createAxon(): CommandServiceClient
    {
        if (false == $this->axon) {
            $this->axon = new CommandServiceClient(
                $this->config['host'] . ':' . $this->config['port'], [
                    'credentials' => ChannelCredentials::createInsecure(),
                ]
            );
        }

        return $this->axon;
    }

    private function parseDsn(string $dsn): array
    {
        $dsn = Dsn::parseFirst($dsn);

        $supportedSchemes = ['axon'];
        if (false == in_array($dsn->getSchemeProtocol(), $supportedSchemes, true)) {
            throw new \LogicException(
                sprintf(
                    'The given scheme protocol "%s" is not supported. It must be one of "%s"',
                    $dsn->getSchemeProtocol(),
                    implode('", "', $supportedSchemes)
                )
            );
        }

        return array_filter(
            array_replace(
                $dsn->getQuery(),
                [
                    'scheme' => $dsn->getSchemeProtocol(),
                    'scheme_extensions' => $dsn->getSchemeExtensions(),
                    'host' => $dsn->getHost(),
                    'port' => $dsn->getPort(),
                    'path' => $dsn->getPath(),
                    'password' => $dsn->getPassword() ?: $dsn->getUser() ?: $dsn->getString('password'),
                ]
            ),
            static function ($value) {
                return null !== $value;
            }
        );
    }

    private function defaultConfig(): array
    {
        return [
            'scheme' => 'axon',
            'scheme_extensions' => [],
            'host' => '127.0.0.1',
            'port' => 8124,
            'path' => null,
            'password' => null,
            'lazy' => true,
            'redelivery_delay' => 300,
        ];
    }
}
