plugins {
    application
}

repositories {
    mavenLocal()
    mavenCentral()
}

dependencies {
    implementation(libs.clickhouseClient)

    // Keep both processors on the classpath so the example can switch between them.
    implementation(libs.jacksonDatabind)
    implementation(libs.gson)

    implementation(libs.slf4jApi)
    runtimeOnly(libs.slf4jSimple)
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

application {
    mainClass = "com.clickhouse.examples.client_v2.json_processors.ClientV2JsonProcessorsExample"
}

tasks.named<JavaExec>("run") {
    listOf("chEndpoint", "chUser", "chPassword", "chDatabase", "jsonProcessor").forEach { key ->
        System.getProperty(key)?.let { value ->
            systemProperty(key, value)
        }
    }
}
