<?php

declare(strict_types=1);

namespace Lytecache\Support;

/**
 * An order-preserving view of a top-level JSON object, for the hash
 * read-modify-write fallback. json_decode() would turn {} into [] when
 * re-encoded and round big integers to floats, so the fallback would
 * rewrite documents differently from the JSON1 path; this keeps every
 * field's exact JSON text and the original key order instead.
 *
 * @internal
 */
final class JsonObject
{
    /** @var list<array{0: string, 1: string}> [key, raw JSON] pairs, in document order */
    public array $fields = [];

    private function index(string $key): int
    {
        foreach ($this->fields as $i => [$k]) {
            if ($k === $key) {
                return $i;
            }
        }

        return -1;
    }

    public function has(string $key): bool
    {
        return $this->index($key) >= 0;
    }

    public function get(string $key): ?string
    {
        $i = $this->index($key);

        return $i >= 0 ? $this->fields[$i][1] : null;
    }

    public function set(string $key, string $rawJson): void
    {
        $i = $this->index($key);
        if ($i >= 0) {
            $this->fields[$i][1] = $rawJson;
        } else {
            $this->fields[] = [$key, $rawJson];
        }
    }

    public function delete(string $key): bool
    {
        $i = $this->index($key);
        if ($i < 0) {
            return false;
        }
        array_splice($this->fields, $i, 1);

        return true;
    }

    public function toJson(): string
    {
        $parts = [];
        foreach ($this->fields as [$k, $v]) {
            $parts[] = json_encode($k, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR).':'.self::compact($v);
        }

        return '{'.implode(',', $parts).'}';
    }

    /** Removes insignificant whitespace, leaving string contents untouched. */
    private static function compact(string $text): string
    {
        $out = '';
        $inString = false;
        $len = strlen($text);
        for ($i = 0; $i < $len; $i++) {
            $ch = $text[$i];
            if ($inString) {
                $out .= $ch;
                if ($ch === '\\') {
                    $out .= $text[++$i] ?? '';
                } elseif ($ch === '"') {
                    $inString = false;
                }
            } elseif ($ch === '"') {
                $inString = true;
                $out .= $ch;
            } elseif (! ctype_space($ch)) {
                $out .= $ch;
            }
        }

        return $out;
    }

    /**
     * Parses JSON text: the object for an object root, otherwise null plus
     * the root's json_type() name. Throws \JsonException on malformed JSON.
     *
     * @return array{0: ?self, 1: string}
     */
    public static function parse(string $text): array
    {
        json_decode($text, flags: JSON_THROW_ON_ERROR); // validates
        $t = trim($text);
        $root = match ($t[0]) {
            '{' => 'object',
            '[' => 'array',
            '"' => 'text',
            't' => 'true',
            'f' => 'false',
            'n' => 'null',
            default => 'number',
        };
        if ($root !== 'object') {
            return [null, $root];
        }

        $obj = new self;
        $i = 1;
        $len = strlen($t);
        $skipWs = static function () use (&$i, $t, $len): void {
            while ($i < $len && ctype_space($t[$i])) {
                $i++;
            }
        };
        $readString = static function () use (&$i, $t): string {
            $start = $i++;
            while ($t[$i] !== '"') {
                if ($t[$i] === '\\') {
                    $i++;
                }
                $i++;
            }
            $i++;

            return substr($t, $start, $i - $start);
        };
        $readValue = static function () use (&$i, $t, $len, $readString): string {
            $start = $i;
            $depth = 0;
            while ($i < $len) {
                $ch = $t[$i];
                if ($ch === '"') {
                    $readString();
                    if ($depth === 0) {
                        break;
                    }

                    continue;
                }
                if ($ch === '{' || $ch === '[') {
                    $depth++;
                } elseif ($ch === '}' || $ch === ']') {
                    if ($depth === 0) {
                        break;
                    }
                    $depth--;
                    if ($depth === 0) {
                        $i++;
                        break;
                    }
                } elseif ($ch === ',' && $depth === 0) {
                    break;
                }
                $i++;
            }

            return trim(substr($t, $start, $i - $start));
        };

        $skipWs();
        while ($t[$i] !== '}') {
            /** @var string $key */
            $key = json_decode($readString(), flags: JSON_THROW_ON_ERROR);
            $skipWs();
            $i++; // colon
            $skipWs();
            $obj->set($key, $readValue());
            $skipWs();
            if ($t[$i] === ',') {
                $i++;
                $skipWs();
            }
        }

        return [$obj, 'object'];
    }
}
