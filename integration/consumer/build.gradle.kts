import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
	kotlin("jvm") version "2.4.21"
	kotlin("plugin.serialization") version "2.4.21"
	application
}

repositories {
	maven { url = uri(providers.gradleProperty("rpcRepository").get()) }
	mavenCentral()
}

dependencies {
	implementation(providers.gradleProperty("rpcCoordinates").get())
}

kotlin {
	jvmToolchain(21)
	compilerOptions {
		jvmTarget.set(JvmTarget.JVM_17)
		freeCompilerArgs.add("-Xjdk-release=17")
	}
}

tasks.withType<JavaCompile>().configureEach {
	options.release.set(17)
}

application {
	mainClass.set("consumer.MainKt")
}

tasks.named<JavaExec>("run") {
	javaLauncher.set(javaToolchains.launcherFor {
		languageVersion.set(JavaLanguageVersion.of(providers.gradleProperty("testJavaVersion").orElse("21").get().toInt()))
	})
}
