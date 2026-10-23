<?php

declare(strict_types=1);

// Usage:
//   php events-worker.php <dbPath> subscribe <pattern> <count> <outFile> <readyFile>
//   php events-worker.php <dbPath> publish <count>
//   php events-worker.php <dbPath> cursorsub <readyFile>   (handles "jobs" forever; killed by the test)

require __DIR__.'/../../vendor/autoload.php';

use Lytecache\LyteCache;

$dbPath = $argv[1];
$mode = $argv[2];
$cache = new LyteCache(path: $dbPath, sweepInterval: null);

if ($mode === 'subscribe') {
    [, , , $pattern, $count, $outFile, $readyFile] = $argv;
    $sub = $cache->watch($pattern);
    file_put_contents($readyFile, 'ready');
    $lines = [];
    while (count($lines) < (int) $count) {
        $e = $sub->poll(30.0);
        if ($e === null) {
            exit(2);
        }
        $lines[] = "{$e->seq} {$e->channel} ".json_encode($e->payload());
    }
    $sub->close();
    file_put_contents($outFile, implode("\n", $lines));
} elseif ($mode === 'publish') {
    for ($i = 0; $i < (int) $argv[3]; $i++) {
        $cache->notify('orders:new', $i);
        $cache->notify('unrelated', $i);
    }
} elseif ($mode === 'cursorsub') {
    $sub = $cache->watch('jobs', fromSeq: $cache->loadCursor('worker'));
    file_put_contents($argv[3], 'ready');
    foreach ($sub as $event) {
        $cache->saveCursor('worker', $event->seq);
    }
}
$cache->close();
exit(0);
