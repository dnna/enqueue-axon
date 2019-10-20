<?php
declare(strict_types=1);

namespace Dnna\Enqueue\Axon;

interface Serializer
{
    public function toString(AxonMessage $message): string;

    public function toMessage(string $string): AxonMessage;
}
