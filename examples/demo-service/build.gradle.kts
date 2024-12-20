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
	implementation("com.clickhouse:client-v2:$ch_java_client_version-SNAPSHOT:all") // local or nightly build
//	implementation("com.clickhouse:client-v2:$ch_java_client_version:all") // release version

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
