<?php

declare(strict_types=1);

namespace Lytecache\Exceptions;

use Lytecache\Subscription;

/**
 * Thrown by a {@see Subscription} when events it should have
 * seen are gone: retention deleted them before it read them, or its cursor
 * is ahead of the file (the file was recreated). Never silently skipped.
 * To accept the loss, watch again with fromSeq: $e->resumeSeq.
 */
final class GapException extends LytecacheException
{
    public readonly int $resumeSeq;

    public function __construct(
        public readonly int $requestedSeq,
        public readonly int $oldestSeq,
        public readonly bool $ahead = false,
    ) {
        $this->resumeSeq = $oldestSeq - 1;
        parent::__construct($ahead
            ? "lytecache: cursor {$requestedSeq} is ahead of the last event ever published (".($oldestSeq - 1).'); the database file was probably recreated'
            : "lytecache: events after seq {$requestedSeq} are no longer retained (oldest retained is {$oldestSeq}); events ".($requestedSeq + 1).'..'.($oldestSeq - 1).' were lost');
    }
}
