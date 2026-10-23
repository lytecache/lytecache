<?php

declare(strict_types=1);

namespace Lytecache;

/**
 * One committed mutation, as delivered to {@see LyteCache::onChange()}
 * listeners. A bulk operation produces ONE record, never one per key:
 * $keys lists the keys the call targeted, or is null with $truncated true
 * when there were more than the configured cap (default 100); flush always
 * has null keys. toArray() is the payload published to "__cache__".
 */
final readonly class ChangeRecord
{
    /**
     * @param  'set'|'delete'|'flush'  $op
     * @param  list<string>|null  $keys
     */
    public function __construct(
        public string $op,
        public string $namespace,
        public ?array $keys,
        public int $count,
        public bool $truncated,
    ) {}

    /**
     * @return array{op: string, namespace: string, keys: list<string>|null, count: int, truncated: bool}
     */
    public function toArray(): array
    {
        return [
            'op' => $this->op,
            'namespace' => $this->namespace,
            'keys' => $this->keys,
            'count' => $this->count,
            'truncated' => $this->truncated,
        ];
    }
}
