<?php

/*
 * Bootstrap guard: refuse to run on a PHP older than this package supports.
 *
 * Why this file exists
 * --------------------
 * composer.json declares "php": "^8.2", and Composer enforces it at install
 * time. But an install can bypass that check (--ignore-platform-reqs,
 * --ignore-platform-req=php, or a root composer.json that sets
 * config.platform.php), and Composer's own vendor/composer/platform_check.php
 * does not help then: it leaves out exactly the requirements that were
 * ignored. Without this guard, a below-floor install loads fine and then dies
 * mid-request with a parse error deep inside the library, the first time it
 * touches a class that uses newer syntax.
 *
 * Composer includes this file (autoload.files) when vendor/autoload.php is
 * loaded, before any lytecache class, so a wrong PHP fails at startup with an
 * explanation instead.
 *
 * Why its syntax is constrained
 * -----------------------------
 * This file runs on the very PHP versions it is rejecting. If it used any
 * syntax those versions can't parse, it would crash with the same kind of
 * parse error it exists to replace. It MUST stay parseable by PHP 7.0 (and
 * ideally 5.x): plain procedural code, PHP_VERSION_ID, string concatenation.
 * No match, enums, arrow functions, typed properties, constructor
 * promotion, readonly, named arguments, nullsafe operator, or
 * declare(strict_types). Do not "modernize" it. CI lints it on PHP 7.0.
 *
 * Keep the version below in step with "php" in composer.json.
 */

if (PHP_VERSION_ID < 80200) {
    throw new RuntimeException(
        'lytecache requires PHP 8.2 or newer; this process is running PHP '.PHP_VERSION.'. '
        .'Composer should have refused this install, so it was most likely installed with '
        .'--ignore-platform-reqs or --ignore-platform-req=php, or the root composer.json sets '
        .'config.platform.php. '
        .'See https://lytecache.github.io/lytecache/#installed-but-crashes'
    );
}
