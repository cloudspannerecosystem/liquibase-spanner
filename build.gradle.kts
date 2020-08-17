import org.gradle.jvm.tasks.Jar


plugins {
    kotlin("jvm") version "1.3.71"
    id("com.google.cloud.tools.jib") version "2.4.0"
    id("com.github.johnrengelman.shadow") version "6.0.0"

}

group = "com.google.spanner"
version = "0.1-SNAPSHOT"

val spek_version = "2.0.10"

repositories {
    mavenCentral()
    jcenter()
    maven(url = "https://oss.sonatype.org/content/groups/public/")
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

sourceSets {
    main {
        java {
            srcDir("src/main/java")
        }
        java {
            srcDir("src/wrapper/kotlin")
        }
    }
    test {
        java {
            srcDir("src/test/java")
        }
    }
}


dependencies {

    // Liquibase Core
    implementation("org.liquibase:liquibase-core:3.8.9")

    // Cloud Spanner related
    implementation("com.google.cloud:google-cloud-spanner-jdbc:1.16.0")
    implementation("com.google.cloud:google-cloud-spanner:1.52.0")
    implementation("com.google.cloud:google-cloud-logging-logback:0.116.0-alpha")

    // Kotlin and wrapper specific
    implementation(kotlin("stdlib-jdk8"))
    implementation("com.github.ajalt:clikt:2.6.0")
    implementation("io.github.microutils:kotlin-logging:1.7.9")
    implementation("org.eclipse.jgit:org.eclipse.jgit:5.7.0.202003110725-r")

    // Testing
    testImplementation("org.spekframework.spek2:spek-dsl-jvm:$spek_version")
    testImplementation("org.spekframework.spek2:spek-runner-junit5:$spek_version")
    testImplementation("org.junit.jupiter:junit-jupiter:5.6.1")
    testImplementation("org.testcontainers:testcontainers:1.13.0")
    testImplementation("org.testcontainers:postgresql:1.13.0")
    testImplementation("org.testcontainers:mysql:1.13.0")
    testImplementation("org.amshove.kluent:kluent:1.60")
}

tasks {

    compileKotlin {
        kotlinOptions.jvmTarget = "1.8"
    }

    compileTestKotlin {
        kotlinOptions.jvmTarget = "1.8"
    }

    // Output to build/libs/shadow.jar
    //
    // This jar can be used directly as a library in the liquibase
    // dependency. It includes everything needed to run with Cloud Spanner
    // and liquibase (JDBC, CloudSpanner SDK, extension, dependencies, etc).
    //
    shadowJar {
        dependsOn("test")
        baseName = "shadow"
        //mergeServiceFiles()
        dependencies {
            exclude(dependency("org.liquibase:liquibase-core"))
        }
    }

    // Output runnable docker container. The docker container
    // contains everything you need to run Liquibase with Spanner.
    jib {
        from {
            image = "gcr.io/distroless/java:8"
        }
        to {
            image = "spanner-liquibase"
        }
    }

    withType<Test> {
        useJUnitPlatform {
            includeEngines("spek2")
        }
    }

    // Self-contained wrapper JAR
    register<Jar>("fatJar2") {
        dependsOn("test")
        group = "application"
        classifier = "all"
        manifest {
            attributes["Implementation-Title"] = "Gradle Jar File Example"
            attributes["Implementation-Version"] = version
            //attributes["Main-Class"] = "com.google.spanner.liquibase.MainKt"
            attributes["Main-Class"] = "com.google.spanner.liquibase.MainKt"
        }
        archiveBaseName.set("${project.name}-fat2")
        from(configurations.runtimeClasspath.get().map({ if (it.isDirectory) it else zipTree(it).matching{
          exclude("META-INF/**")
        }
        }))
        with(jar.get() as CopySpec)
    }
}
