plugins {
    `java-library`
    `maven-publish`
    signing
}

group = "io.litecache"
version = "0.1.0"

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
    api("org.xerial:sqlite-jdbc:3.44.0.0")
    api("com.fasterxml.jackson.core:jackson-databind:2.16.1")
    api("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.16.1")

    // Test dependencies
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.1")
    testImplementation("org.assertj:assertj-core:3.24.1")
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
            artifactId = "litecache"
            from(components["java"])

            pom {
                name.set("LiteCache")
                description.set("Redis-like embedded caching library backed by SQLite: zero-config, portable JSON, production-grade concurrency.")
                url.set("https://github.com/lytecache/litecache-java")

                licenses {
                    license {
                        name.set("Apache License 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }

                developers {
                    developer {
                        id.set("litecache")
                        name.set("LiteCache Team")
                        email.set("litecache@users.noreply.github.com")
                    }
                }

                scm {
                    connection.set("scm:git:https://github.com/lytecache/litecache-java.git")
                    developerConnection.set("scm:git:ssh://git@github.com/lytecache/litecache-java.git")
                    url.set("https://github.com/lytecache/litecache-java")
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

        // Sonatype Central Portal (https://central.sonatype.com), via its OSSRH-compatible staging
        // endpoint. Only registered when credentials are supplied (CI release job), so plain
        // `./gradlew build` / `publishToMavenLocal` never require them.
        val sonatypeUsername = providers.gradleProperty("sonatypeUsername").orNull
            ?: System.getenv("SONATYPE_USERNAME")
        val sonatypePassword = providers.gradleProperty("sonatypePassword").orNull
            ?: System.getenv("SONATYPE_PASSWORD")
        if (sonatypeUsername != null && sonatypePassword != null) {
            maven {
                name = "SonatypeCentral"
                url = uri("https://ossrh-staging-api.central.sonatype.com/service/local/staging/deploy/maven2/")
                credentials {
                    username = sonatypeUsername
                    password = sonatypePassword
                }
            }
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
