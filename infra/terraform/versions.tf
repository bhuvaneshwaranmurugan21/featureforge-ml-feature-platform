terraform {
  required_version = "= 1.9.8"

  backend "s3" {}

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "= 5.100.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      ManagedBy = "Terraform"
      Project   = "FeatureForge"
      RunId     = var.run_id
      Stage     = "6"
    }
  }
}
