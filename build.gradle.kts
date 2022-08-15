import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

buildscript {
  repositories {
    mavenCentral()
  }
}

plugins {
  `java-library`
  idea
  jacoco
  signing
  `maven-publish`
  id("com.github.kt3k.coveralls") version "2.12.0"
  id("com.github.johnrengelman.shadow") version "7.1.2"
}

subprojects {
  group = "org.komamitsu"
  apply(from = "../version.gradle")
  apply(plugin = "java-library")
  apply(plugin = "idea")
  apply(plugin = "com.github.johnrengelman.shadow")
  apply(plugin = "signing")
  apply(plugin = "maven-publish")
  apply(plugin = "jacoco")
  apply(plugin = "com.github.kt3k.coveralls")

  repositories {
    mavenCentral()
    mavenLocal()
  }

  dependencies {
    implementation("org.slf4j:slf4j-api:1.7.36")
    implementation("org.msgpack:jackson-dataformat-msgpack:0.9.1")
    implementation("org.komamitsu:phi-accural-failure-detector:0.0.5")
    implementation("net.jodah:failsafe:2.4.4")

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.8.2")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:5.8.2")
    testImplementation("org.junit.jupiter:junit-jupiter-params:5.8.2")
    testImplementation("ch.qos.logback:logback-classic:1.2.11")
    testImplementation("org.hamcrest:hamcrest-all:1.3")
    testImplementation("org.mockito:mockito-core:4.7.0")
    testImplementation("com.google.guava:guava:31.1-jre")
  }

  base {
    archivesBaseName = "fluency"
  }

  java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8

    withJavadocJar()
    withSourcesJar()
  }

  tasks.withType<Test> {
    useJUnitPlatform()
  }

  tasks.withType<ShadowJar> {
    relocate("com.fasterxml.jackson", "org.komamitsu.thirdparty.jackson")
    relocate("org.msgpack.jackson", "org.komamitsu.thirdparty.msgpack.jackson")
    classifier = "shadow"
  }

  publishing {
    publications {
      create<MavenPublication>("maven") {
        from(components["java"])

        pom {
          name.set("fluency")
          description.set("High throughput data ingestion logger to Fluentd and Treasure Data")
          url.set("https://github.com/komamitsu/fluency")
          licenses {
            license {
              name.set("The Apache License, Version 2.0")
              url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
            }
          }
          developers {
            developer {
              id.set("komamitsu")
              name.set("Mitsunori Komatsu")
              email.set("komamitsu@gmail.com")
            }
          }
          scm {
            connection.set("scm:git:git://github.com/komamitsu/fluency.git")
            developerConnection.set("scm:git:git@github.com:komamitsu/fluency.git")
            url.set("https://github.com/komamitsu/fluency")
          }
        }
      }
    }
    repositories {
      maven {
        url = uri(if (project.version.toString().endsWith("-SNAPSHOT")) {
            "https://oss.sonatype.org/content/repositories/snapshots/"
          }
          else {
            "https://oss.sonatype.org/service/local/staging/deploy/maven2/"
          })

        credentials {
          username = if (project.hasProperty("ossrhUsername")) {
            project.property("ossrhUsername").toString()
          }
          else {
            ""
          }

          password = if (project.hasProperty("ossrhPassword")) {
            project.property("ossrhPassword").toString()
          }
          else {
            ""
          }
        }
      }
    }
  }

  signing {
    if (project.hasProperty("signing.gnupg.keyName")) {
      setRequired(true)
    }
    else if (project.hasProperty("signingKey")) {
      val signingKeyId = project.property("signingKeyId").toString()
      val signingKey = project.property("signingKey").toString()
      val signingPassword = project.property("signingPassword").toString()
      useInMemoryPgpKeys(signingKeyId, signingKey, signingPassword)
      setRequired(true)
    }
    else {
      setRequired(false)
    }
    sign(publishing.publications["maven"])
    sign(configurations.archives.get())
  }

  val publishedProjects = project.subprojects

  val jacocoRootReport by tasks.creating(JacocoReport::class) {
    description = "Generates an aggregate report from all subprojects"
    group = "Coverage reports"
    val jacocoReportTasks = publishedProjects
      .map { it.tasks["jacocoTestReport"] as JacocoReport }
      .toList()
    dependsOn(jacocoReportTasks)

    executionData.setFrom(Callable { jacocoReportTasks.map { it.executionData } })

    publishedProjects.forEach { testedProject ->
      val mainSourceSet = testedProject.sourceSets["main"]
      additionalSourceDirs(mainSourceSet.allSource.sourceDirectories)
      additionalClassDirs(mainSourceSet.output)
    }

    reports {
      xml.isEnabled = true
      html.isEnabled = true
    }
  }

  coveralls {
    sourceDirs = publishedProjects.map { proj ->
        proj.sourceSets["main"].allSource.sourceDirectories.map {
          it.toString()
        }
      }.flatten()
    jacocoReportPath = "${buildDir}/reports/jacoco/jacocoRootReport/jacocoRootReport.xml"
  }

  tasks.coveralls {
    dependsOn(jacocoRootReport)
  }
}

