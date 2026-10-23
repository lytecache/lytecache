<?php

declare(strict_types=1);

namespace Lytecache\Tests\Unit;

use Lytecache\Tests\TestCase;

/**
 * The bootstrap guard (src/platform_guard.php) must agree with composer.json
 * and stay first in autoload.files. CI separately runs it on PHP 7.0 and
 * 8.1 to prove it parses there and refuses clearly.
 */
final class PlatformGuardTest extends TestCase
{
    private const ROOT = __DIR__.'/../..';

    public function test_guard_floor_matches_the_composer_constraint(): void
    {
        $composer = json_decode((string) file_get_contents(self::ROOT.'/composer.json'), true, flags: JSON_THROW_ON_ERROR);
        self::assertIsArray($composer);
        $constraint = $composer['require']['php'];
        self::assertMatchesRegularExpression('/^\^(\d+)\.(\d+)$/', $constraint, 'the guard assumes a single ^X.Y floor');
        preg_match('/^\^(\d+)\.(\d+)$/', $constraint, $m);
        $floorId = (int) $m[1] * 10000 + (int) $m[2] * 100;

        $guard = (string) file_get_contents(self::ROOT.'/src/platform_guard.php');
        self::assertStringContainsString("PHP_VERSION_ID < {$floorId}", $guard);
        self::assertStringContainsString("requires PHP {$m[1]}.{$m[2]} or newer", $guard);
    }

    public function test_guard_is_the_first_autoloaded_file(): void
    {
        $composer = json_decode((string) file_get_contents(self::ROOT.'/composer.json'), true, flags: JSON_THROW_ON_ERROR);
        self::assertIsArray($composer);
        self::assertSame('src/platform_guard.php', $composer['autoload']['files'][0] ?? null);
    }

    public function test_guard_is_silent_on_a_supported_php(): void
    {
        $result = require self::ROOT.'/src/platform_guard.php';
        self::assertSame(1, $result); // included without throwing
    }
}
