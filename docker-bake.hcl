group "default" {
  targets = ["tsdb-amd64", "tsdb-arm64", "tsdb-armv7"]
}

target "tsdb-amd64" {
  dockerfile = "Dockerfile"
  platforms = ["linux/amd64"]
  tags = ["tsdb2:latest-amd64"]
}

target "tsdb-arm64" {
  dockerfile = "Dockerfile"
  platforms = ["linux/arm64"]
  tags = ["tsdb2:latest-arm64"]
}

target "tsdb-armv7" {
  dockerfile = "Dockerfile"
  platforms = ["linux/arm/v7"]
  tags = ["tsdb2:latest-armv7"]
}
