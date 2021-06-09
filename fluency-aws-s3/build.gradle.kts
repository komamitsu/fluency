dependencies {
  implementation(project(":fluency-core"))

  implementation("software.amazon.awssdk:s3:2.16.77")
}

publishing {
  publications {
    create<MavenPublication>("maven") {
      artifactId = "fluency-aws-s3"
    }
  }
}
