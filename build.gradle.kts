plugins {
    id("java")
    id("com.google.protobuf") version("0.9.4")
    id("jacoco")
}

group = "com.github.lukaszbudnik"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.rocksdb:rocksdbjni:9.10.0")
    implementation("org.msgpack:jackson-dataformat-msgpack:0.9.9")
    implementation("commons-io:commons-io:2.15.1")

    implementation("com.google.protobuf:protobuf-java:4.30.2")
    implementation("com.google.protobuf:protobuf-java-util:4.30.2")
    implementation("io.grpc:grpc-protobuf:1.71.0")
    implementation("io.grpc:grpc-stub:1.71.0")
    implementation("io.grpc:grpc-netty-shaded:1.71.0")
    implementation("javax.annotation:javax.annotation-api:1.3.2")

    implementation("org.slf4j:slf4j-api:2.0.17")

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("io.grpc:grpc-inprocess:1.71.0")
    testImplementation("ch.qos.logback:logback-classic:1.5.18")
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
            artifact = "io.grpc:protoc-gen-grpc-java:1.71.0"
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
}

tasks.test {
    finalizedBy(tasks.jacocoTestReport)
}
tasks.jacocoTestReport {
    dependsOn(tasks.test)

    classDirectories.setFrom(
        files(classDirectories.files.map {
            fileTree(it) {
                exclude(
                    "com/github/lukaszbudnik/roxdb/proto/**"
                )
            }
        })
    )
}