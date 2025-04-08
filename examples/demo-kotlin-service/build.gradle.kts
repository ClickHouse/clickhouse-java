
val kotlin_version: String by project
val logback_version: String by project
val ktor_version: String by project

plugins {
    kotlin("jvm") version "2.0.20"
    id("io.ktor.plugin") version "2.3.12"
}

group = "com.clickhouse"
version = "0.0.1"

application {
    mainClass.set("io.ktor.server.netty.EngineMain")

    val isDevelopment: Boolean = project.ext.has("development")
    applicationDefaultJvmArgs = listOf("-Dio.ktor.development=$isDevelopment")
}

repositories {
    mavenLocal() // comment to pull nightly builds instead of local cache
    mavenCentral()
    maven("https://s01.oss.sonatype.org/content/repositories/snapshots/") // for nightly builds
}

val ch_java_client_version: String by extra

dependencies {
    // application dependencies
    implementation("io.ktor:ktor-server-core-jvm")
    implementation("io.ktor:ktor-server-netty-jvm")
    implementation("ch.qos.logback:logback-classic:$logback_version")
    implementation("io.ktor:ktor-server-config-yaml")
    implementation("io.ktor:ktor-server-content-negotiation:$ktor_version")
    implementation("io.ktor:ktor-serialization-kotlinx-json:$ktor_version")

    // https://mvnrepository.com/artifact/com.clickhouse/client-v2
    implementation("com.clickhouse:client-v2:${ch_java_client_version}-SNAPSHOT:all")
//    implementation("com.clickhouse:client-v2:${ch_java_client_version}:all") // release version

    testImplementation("io.ktor:ktor-server-test-host-jvm")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit:$kotlin_version")
}
