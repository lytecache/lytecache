<?php

declare(strict_types=1);

namespace Lytecache\Support;

/**
 * The PDO statement class every LyteCache connection uses (via
 * PDO::ATTR_STATEMENT_CLASS), so that reusing a cached statement is safe no
 * matter how its previous execution ended.
 *
 * Why it exists: when a statement's execute() fails (a constraint violation,
 * a busy error, a trigger), pdo_sqlite leaves the underlying SQLite statement
 * un-reset. The next bindValue() + execute() on it then returns false
 * *without throwing*, even in ERRMODE_EXCEPTION, and the write never
 * happens. Because LyteCache caches its prepared statements, that turned one
 * failed write into every later write through the same statement being
 * silently dropped (fixed in 0.3.1; see tests/Feature/WriteAfterFailedWriteTest).
 *
 * Resetting here, before every execution, is unconditional: it doesn't depend
 * on any caller having a correct finally block on every exit path, and it
 * also covers read statements, whose results are consumed after the calling
 * scope returns. A failed execution can therefore never be carried into the
 * next one. The false check is a second line of defence: a write is never
 * allowed to fail silently again.
 *
 * @internal
 */
final class Statement extends \PDOStatement
{
    /** @param array<int|string, mixed>|null $params */
    public function execute(?array $params = null): bool
    {
        $this->closeCursor();

        if (! parent::execute($params)) {
            $info = $this->errorInfo();

            throw new \PDOException('lytecache: statement execution failed without an exception: '
                .($info[2] ?? 'unknown SQLite error').' ('.$this->queryString.')');
        }

        return true;
    }
}
