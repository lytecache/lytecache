plugins {
    `java-library`
    `maven-publish`
    signing
    id("io.github.gradle-nexus.publish-plugin") version "2.0.0"
}

group = "io.github.lytecache"
version = "0.2.0"

repositories {
    mavenCentral()
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
    withSourcesJar()
    withJavadocJar()
}

dependencies {
    // Runtime dependencies (exactly as spec'd)
    api("org.xerial:sqlite-jdbc:3.53.4.0")
    api("com.fasterxml.jackson.core:jackson-databind:2.16.1")
    api("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.16.1")

    // Test dependencies
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.1")
    testImplementation("org.assertj:assertj-core:3.24.1")
    // Explicit per Gradle 9's removal of automatic test-framework-dependency loading.
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}

tasks.javadoc {
    val docletOptions = options as StandardJavadocDocletOptions
    docletOptions.addBooleanOption("Xwerror", true)
    docletOptions.addStringOption("Xdoclint:all", "-quiet")
    isFailOnError = true
}

tasks.compileJava {
    options.compilerArgs.add("-Werror")
    options.compilerArgs.add("-Xlint:all,-processing")
}

tasks.jar {
    manifest {
        attributes(
            "Implementation-Title" to project.name,
            "Implementation-Version" to project.version,
            "Implementation-Vendor-Id" to project.group,
            "Specification-Title" to project.name,
            "Specification-Version" to project.version
        )
    }
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            artifactId = "lytecache"
            from(components["java"])

            pom {
                name.set("LyteCache")
                description.set("Redis-like embedded caching library backed by SQLite: zero-config, portable JSON, production-grade concurrency.")
                url.set("https://lytecache.github.io/lytecache")

                licenses {
                    license {
                        name.set("Apache License 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }

                developers {
                    developer {
                        id.set("lytecache")
                        name.set("LyteCache Team")
                        email.set("lytecache@users.noreply.github.com")
                    }
                }

                scm {
                    connection.set("scm:git:https://github.com/lytecache/lytecache-java.git")
                    developerConnection.set("scm:git:ssh://git@github.com/lytecache/lytecache-java.git")
                    url.set("https://github.com/lytecache/lytecache-java")
                }
            }
        }
    }

    repositories {
        // A plain file-based repo, handy for local inspection of what would be published
        // (./gradlew publish also always installs to ~/.m2 via publishToMavenLocal).
        maven {
            name = "BuildDir"
            url = uri(layout.buildDirectory.dir("repo"))
        }

        // Sonatype Central Portal itself is configured below via nexusPublishing, not here --
        // plain maven-publish can only upload artifacts into a new staging repository, it has no
        // way to make the mandatory follow-up call that actually closes and releases that staging
        // repository (without it, the upload silently succeeds but never becomes visible on
        // Sonatype's Deployments page or on Maven Central at all). The gradle-nexus/publish-plugin
        // task chain (publishToSonatype + closeAndReleaseSonatypeStagingRepository) drives that
        // whole lifecycle instead of a plain publish task.
    }
}

nexusPublishing {
    repositories {
        sonatype {
            // The OSSRH-compatible staging endpoint (OSSRH itself shut down June 2025; this
            // bridge is Sonatype's documented continuation path for maven-publish-based setups).
            nexusUrl.set(uri("https://ossrh-staging-api.central.sonatype.com/service/local/"))
            snapshotRepositoryUrl.set(uri("https://central.sonatype.com/repository/maven-snapshots/"))
            // Same credential-optional pattern as elsewhere in this file: absent locally, supplied
            // as CI secrets during a release. Property/Provider values stay unresolved until a
            // Sonatype-specific task actually runs, so plain `./gradlew build` never needs them.
            username.set(providers.gradleProperty("sonatypeUsername").orElse(providers.environmentVariable("SONATYPE_USERNAME")))
            password.set(providers.gradleProperty("sonatypePassword").orElse(providers.environmentVariable("SONATYPE_PASSWORD")))
        }
    }
}

signing {
    // In-memory ASCII-armored key + passphrase, supplied as Gradle properties or env vars
    // (GitHub Actions secrets) during a release. Signing is skipped -- not failed -- when they're
    // absent, so `build` and `publishToMavenLocal` work with no secrets configured.
    val signingKey = providers.gradleProperty("signingKey").orNull
        ?: System.getenv("SIGNING_KEY")
    val signingPassword = providers.gradleProperty("signingPassword").orNull
        ?: System.getenv("SIGNING_PASSWORD")
    isRequired = signingKey != null && signingPassword != null
    if (isRequired) {
        useInMemoryPgpKeys(signingKey, signingPassword)
        sign(publishing.publications["mavenJava"])
    }
}
