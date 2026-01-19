plugins {
    id("java")
    id("com.google.protobuf") version ("0.9.4")
    id("jacoco")
    id("application")
    id("com.gradleup.shadow") version "9.3.1"
}

group = "com.github.lukaszbudnik"
version = "1.0-SNAPSHOT"

application {
    mainClass.set("com.github.lukaszbudnik.roxdb.application.Application")
}

repositories {
    mavenCentral()
}

val mockitoAgent = configurations.create("mockitoAgent")

val os = System.getProperty("os.name").lowercase()
val classifier = when {
    os.contains("mac") -> "osx"
    os.contains("win") -> "win64"
    else -> "linux64"
}

println("Detected OS: $os, RocksDB classifier: $classifier")

dependencies {
    implementation(libs.rocksdb.map { r -> "${r.group}:${r.name}:${r.version}:${classifier}" })
    implementation(libs.kryo)
    implementation(libs.commons.io)

    implementation(libs.protobuf.java)
    implementation(libs.protobuf.java.util)
    implementation(libs.grpc.protobuf)
    implementation(libs.grpc.services)
    implementation(libs.grpc.stub)
    implementation(libs.grpc.netty.shaded)
    implementation(libs.javax.annotation)

    implementation(libs.slf4j.api)
    implementation(libs.logback.classic)

    implementation(libs.bundles.opentelemetry)
    implementation(libs.jackson.dataformat.yaml)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.grpc.inprocess)
    testImplementation(libs.mockito.junit)
    testImplementation(libs.bouncycastle.bcprov)
    testImplementation(libs.opentelemetry.sdk.testing)

    mockitoAgent(libs.mockito.core) { isTransitive = false }
}


java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:4.33.2"
    }
    plugins {
        create("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.78.0"
        }
    }
    generateProtoTasks {
        all().forEach {
            it.plugins {
                create("grpc")
            }
        }
    }
}

sourceSets {
    main {
        java {
            srcDirs("build/generated/source/proto/main/java")
            srcDirs("build/generated/source/proto/main/grpc")
        }
    }
}

tasks.test {
    useJUnitPlatform()
    jvmArgs("-javaagent:${mockitoAgent.asPath}")
}

tasks.test {
    finalizedBy(tasks.jacocoTestReport)
}

tasks.jacocoTestReport {

    reports {
        xml.required.set(true)
    }

    dependsOn(tasks.test)

    classDirectories.setFrom(
        files(classDirectories.files.map {
            fileTree(it) {
                exclude(
                    "com/github/lukaszbudnik/roxdb/v1/**"
                )
            }
        })
    )
}

