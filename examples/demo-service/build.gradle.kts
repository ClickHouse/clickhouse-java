plugins {
	java
	id("org.springframework.boot") version "3.4.4"
	id("io.spring.dependency-management") version "1.1.7"
}

group = "com.clickhouse"
version = "0.0.1-SNAPSHOT"

java {
	toolchain {
		languageVersion = JavaLanguageVersion.of(17)
	}
}

configurations {
	compileOnly {
		extendsFrom(configurations.annotationProcessor.get())
	}
}

repositories {
	mavenLocal() // comment to pull nightly builds instead of local cache
	mavenCentral()
	maven("https://s01.oss.sonatype.org/content/repositories/snapshots/") // for nightly builds
}

val ch_java_client_version: String by extra

dependencies {

	// Add this if working with client directly (not JDBC)
//	implementation("com.clickhouse:client-v2:${ch_java_client_version}-SNAPSHOT:all") // local or nightly build
//	implementation("com.clickhouse:client-v2:${ch_java_client_version}:all") // release version

	// OR this if working with JDBC (or both)
	implementation("com.clickhouse:clickhouse-jdbc:${ch_java_client_version}-SNAPSHOT:all") // local or nightly build
//	implementation("com.clickhouse:clickhouse-jdbc:${ch_java_client_version}:all") // release version

	// Other dependencies
	implementation("org.springframework.boot:spring-boot-starter-actuator")
	implementation("org.springframework.boot:spring-boot-starter-web")

	// To enable JPA
	implementation("org.springframework.boot:spring-boot-starter-data-jpa")

	compileOnly("org.projectlombok:lombok")
	annotationProcessor("org.projectlombok:lombok")

	implementation("io.micrometer:micrometer-core:1.14.3")

	// -- test dependencies
	testImplementation("org.springframework.boot:spring-boot-starter-test")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<Test> {
	useJUnitPlatform()
}
