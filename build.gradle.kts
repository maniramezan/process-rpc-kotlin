import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.dsl.abi.ExperimentalAbiValidation

plugins {
	kotlin("jvm") version "2.4.21"
	kotlin("plugin.serialization") version "2.4.21"
	id("org.jetbrains.dokka") version "2.2.0"
	`java-library`
	`maven-publish`
}

dokka {
	dokkaPublications.html {
		moduleName.set("Process RPC for Kotlin")
		moduleVersion.set(project.version.toString())
		outputDirectory.set(layout.buildDirectory.dir("documentation/html"))
		includes.from("README.md", "docs/architecture.md")
	}
	dokkaSourceSets.main {
		sourceLink {
			localDirectory.set(file("src/main/kotlin"))
			remoteUrl.set(uri("https://github.com/maniramezan/process-rpc-kotlin/tree/main/src/main/kotlin"))
			remoteLineSuffix.set("#L")
		}
	}
}

kotlin {
	jvmToolchain(21)
	explicitApi()
	compilerOptions {
		jvmTarget.set(JvmTarget.JVM_17)
		freeCompilerArgs.add("-Xjdk-release=17")
	}
	@OptIn(ExperimentalAbiValidation::class)
	abiValidation()
}

java {
	withSourcesJar()
}

tasks.withType<JavaCompile>().configureEach {
	options.release.set(17)
}

val testJavaVersion = providers.gradleProperty("testJavaVersion").map(String::toInt).orElse(21)

tasks.withType<Test>().configureEach {
	javaLauncher.set(javaToolchains.launcherFor {
		languageVersion.set(JavaLanguageVersion.of(testJavaVersion.get()))
	})
}

dependencies {
	api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")
	api("org.jetbrains.kotlinx:kotlinx-serialization-json:1.12.0")
	testImplementation(kotlin("test"))
}

publishing {
	repositories {
		maven {
			name = "consumerSmoke"
			url = layout.buildDirectory.dir("consumer-repository").get().asFile.toURI()
		}
	}
	publications {
		create<MavenPublication>("maven") {
			from(components["java"])
			artifactId = "process-rpc"
			pom {
				name.set("Process RPC")
				description.set("Supervised JSON-RPC communication with bundled child processes")
				url.set("https://github.com/maniramezan/process-rpc-kotlin")
				licenses {
					license {
						name.set("MIT License")
						url.set("https://opensource.org/license/mit")
					}
				}
			}
		}
	}
}

tasks.register<GradleBuild>("consumerSmokeTest") {
	group = "verification"
	description = "Publishes to a build-local Maven repository and runs an isolated Kotlin consumer."
	dependsOn("publishMavenPublicationToConsumerSmokeRepository")
	dir = file("integration/consumer")
	tasks = listOf("run")
	startParameter.projectProperties = gradle.startParameter.projectProperties + mapOf(
		"rpcRepository" to layout.buildDirectory.dir("consumer-repository").get().asFile.absolutePath,
		"rpcCoordinates" to "${project.group}:process-rpc:${project.version}",
		"testJavaVersion" to testJavaVersion.get().toString(),
	)
}
