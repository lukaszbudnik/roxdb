plugins {
    id("java")
    id("com.google.protobuf") version ("0.9.4")
    id("jacoco")
    id("application")
    id("com.gradleup.shadow") version "8.3.6"
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

dependencies {
    implementation("org.rocksdb:rocksdbjni:10.0.1")
    implementation("com.esotericsoftware:kryo:5.6.2")
    implementation("commons-io:commons-io:2.19.0")

    implementation("com.google.protobuf:protobuf-java:4.30.2")
    implementation("com.google.protobuf:protobuf-java-util:4.30.2")
    implementation("io.grpc:grpc-protobuf:1.71.0")
    implementation("io.grpc:grpc-services:1.72.0")
    implementation("io.grpc:grpc-stub:1.72.0")
    implementation("io.grpc:grpc-netty-shaded:1.71.0")
    implementation("javax.annotation:javax.annotation-api:1.3.2")

    implementation("org.slf4j:slf4j-api:2.0.17")
    implementation("ch.qos.logback:logback-classic:1.5.18")

    testImplementation(platform("org.junit:junit-bom:5.12.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("io.grpc:grpc-inprocess:1.72.0")
    testImplementation("org.mockito:mockito-junit-jupiter:5.17.0")

    mockitoAgent("org.mockito:mockito-core:5.17.0") { isTransitive = false }
}

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:4.30.2"
    }
    plugins {
        create("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.72.0"
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

