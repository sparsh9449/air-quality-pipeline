# Terraform scaffold — written, not deployed. Local Docker provider as a placeholder
# so the project demonstrates IaC intent without incurring any cloud cost.

terraform {
  required_version = ">= 1.6"
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
  }
}

provider "docker" {}

# Example: a Redpanda container could be declared here instead of docker-compose.
# resource "docker_container" "redpanda" { ... }
