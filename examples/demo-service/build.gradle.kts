plugins {
	java
	id("org.springframework.boot") version "3.3.2"
	id("io.spring.dependency-management") version "1.1.6"
}

group = "com.clickhouse"
version = "0.0.1-SNAPSHOT"

java {
	toolchain {
		languageVersion = JavaLanguageVersion.of(21)
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

dependencies {

	// -- clickhouse dependencies
	// Main dependency
	implementation("com.clickhouse:client-v2:0.6.5-SNAPSHOT") // nightly build
//	implementation("com.clickhouse:client-v2:0.6.5") // stable version
	// http client used by clickhouse client
	implementation("org.apache.httpcomponents.client5:httpclient5:5.3.1")
	// compression dependencies
	runtimeOnly("org.apache.commons:commons-compress:1.26.2")
	runtimeOnly("org.lz4:lz4-pure-java:1.8.0")
	// client V1 if old implementation is needed
//	implementation("com.clickhouse:clickhouse-http-client:0.6.5")



	// -- application dependencies
	implementation("org.springframework.boot:spring-boot-starter-actuator")
	implementation("org.springframework.boot:spring-boot-starter-web")
	compileOnly("org.projectlombok:lombok")
	annotationProcessor("org.projectlombok:lombok")

	// -- test dependencies
	testImplementation("org.springframework.boot:spring-boot-starter-test")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<Test> {
	useJUnitPlatform()
}
