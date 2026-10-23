<?php

declare(strict_types=1);

// Usage: php hash-worker.php <dbPath> <hset|hincrby> <key> <field> <iterations>
// Re-exec'd by HashTest to prove field writes from separate processes never lose each other.

require __DIR__.'/../../vendor/autoload.php';

use Lytecache\LyteCache;

[, $dbPath, $mode, $key, $field, $iterations] = $argv;

$cache = new LyteCache(path: $dbPath, sweepInterval: null, logger: static fn (string $m) => null);
for ($i = 0; $i < (int) $iterations; $i++) {
    if ($mode === 'hset') {
        $cache->hset($key, $field, $i);
    } else {
        $cache->hincrBy($key, $field, 1);
    }
}
$cache->close();
exit(0);
