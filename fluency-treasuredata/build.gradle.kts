dependencies {
  implementation(projects.fluencyCore)

  implementation("com.treasuredata.client:td-client:1.1.1")
  implementation("com.google.guava:guava:33.4.0-jre")
  testImplementation("com.fasterxml.jackson.datatype:jackson-datatype-json-org:2.16.1")
  testImplementation("com.fasterxml.jackson.datatype:jackson-datatype-jdk8:2.18.2")
  implementation("com.squareup.okhttp3:okhttp:3.14.9")
}
