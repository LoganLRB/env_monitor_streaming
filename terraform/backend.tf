terraform {
  backend "s3" {
    bucket         = "env-monitor-terraform-state"
    key            = "env-monitor-streaming/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "env-monitor-terraform-locks"
    encrypt        = true
  }
}
