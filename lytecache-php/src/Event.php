<?php

declare(strict_types=1);

namespace Lytecache;

use Lytecache\Support\Serializer;

/**
 * One event published with {@see LyteCache::notify()}. The payload is
 * decoded on demand, with exactly the rules used for cached values.
 */
final readonly class Event
{
    public function __construct(
        public int $seq,
        public string $channel,
        public string $namespace,
        /** Unix milliseconds. */
        public int $createdAt,
        private ?string $rawPayload,
        public int $valueType,
    ) {}

    /** The decoded payload (null for a null payload). */
    public function payload(): mixed
    {
        if ($this->rawPayload === null) {
            return null;
        }

        return Serializer::decode($this->rawPayload, $this->valueType);
    }
}
