terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  backend "s3" {
    bucket = "de-team-rossolimo-terraform-state"
    key = "terraform.tfstate"
    region = "eu-west-2"
  }
}

provider "aws" {
  region = "eu-west-2"
  default_tags {
    tags = {
      ProjectName = "de-team-rossolimo-project"
      Team = "Rossolimo"
      DeployedFrom = "Terraform"
      Repository = "de-rossolimo-project"
      Environment = "dev"
      TeamMembers = "Michael-Mostyn-Nick-Leonette-Heiman"
    }
  }
}
