dependencies {
  testImplementation(project(":fluency-core"))

  implementation(project(":fluency-core"))
}

publishing {
  publications {
    create<MavenPublication>("maven") {
      artifactId = "fluency-fluentd"
    }
  }
}
