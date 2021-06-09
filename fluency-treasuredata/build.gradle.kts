dependencies {
  implementation(project(":fluency-core"))

  implementation("com.treasuredata.client:td-client:0.9.5")
}

publishing {
  publications {
    create<MavenPublication>("maven") {
      artifactId = "fluency-treasuredata"
    }
  }
}
