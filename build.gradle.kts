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

dependencies {
	api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")
	api("org.jetbrains.kotlinx:kotlinx-serialization-json:1.12.0")
	testImplementation(kotlin("test"))
}

publishing {
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
