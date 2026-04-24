plugins {
    application
}

repositories {
    mavenLocal()
    mavenCentral()
}

dependencies {
    implementation(libs.jdbcV2)

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
    mainClass = "com.clickhouse.examples.jdbc_v2.json_processors.JdbcV2JsonProcessorsExample"
}

tasks.named<JavaExec>("run") {
    listOf("chUrl", "chUser", "chPassword", "jsonProcessor").forEach { key ->
        System.getProperty(key)?.let { value ->
            systemProperty(key, value)
        }
    }
}
