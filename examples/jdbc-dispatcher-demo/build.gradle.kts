import java.net.URI

plugins {
    java
    application
}

group = "com.clickhouse.examples"
version = "1.0.0-SNAPSHOT"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

repositories {
    mavenLocal()
    mavenCentral()
    maven("https://central.sonatype.com/repository/maven-snapshots/")
}

val ch_java_client_version: String by extra

dependencies {
    // JDBC Dispatcher from local build or Maven Central
    implementation("com.clickhouse:jdbc-dispatcher:${ch_java_client_version}-SNAPSHOT")

    // SLF4J for logging
    implementation("org.slf4j:slf4j-api:2.0.16")
    implementation("org.slf4j:slf4j-simple:2.0.16")
}

application {
    mainClass.set("com.clickhouse.examples.dispatcher.DispatcherDemo")
}

// Task to run the HTTP service
tasks.register<JavaExec>("runService") {
    group = "application"
    description = "Runs the HTTP backend service on port 8080"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.clickhouse.examples.dispatcher.DispatcherService")
    dependsOn("downloadDrivers")
}

// Task to download driver JARs from GitHub releases
tasks.register("downloadDrivers") {
    group = "setup"
    description = "Downloads ClickHouse JDBC driver JARs from GitHub releases"

    val driversDir = layout.projectDirectory.dir("drivers")

    doLast {
        val drivers = mapOf(
            "clickhouse-jdbc-0.9.6-all.jar" to "https://github.com/ClickHouse/clickhouse-java/releases/download/v0.9.6/clickhouse-jdbc-0.9.6-all.jar",
            "clickhouse-jdbc-0.7.2-all.jar" to "https://github.com/ClickHouse/clickhouse-java/releases/download/v0.7.2/clickhouse-jdbc-0.7.2-all.jar"
        )

        driversDir.asFile.mkdirs()

        drivers.forEach { (filename, url) ->
            val targetFile = driversDir.file(filename).asFile
            if (!targetFile.exists()) {
                println("Downloading $filename from $url")
                URI.create(url).toURL().openStream().use { input ->
                    targetFile.outputStream().use { output ->
                        input.copyTo(output)
                    }
                }
                println("Downloaded $filename (${targetFile.length() / 1024} KB)")
            } else {
                println("$filename already exists, skipping download")
            }
        }
    }
}

tasks.named("run") {
    dependsOn("downloadDrivers")
}
