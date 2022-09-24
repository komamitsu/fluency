java {
  sourceCompatibility = JavaVersion.VERSION_16
  targetCompatibility = JavaVersion.VERSION_16
}

tasks.withType<JavaCompile> {
  options.release.set(16)
}

dependencies {
  implementation(projects.fluencyCore)
  implementation(projects.fluencyFluentd)
}
