variable "project" {
  description = "Project name, used as a prefix for all resources"
  type        = string
  default     = "env-monitor-streaming"
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "kafka_version" {
  description = "MSK Kafka version"
  type        = string
  default     = "3.7.x"
}

variable "api_base_url" {
  description = "Base URL of env_monitor_api (written to SSM for the ECS task)"
  type        = string
}

variable "alert_email" {
  description = "Email address to subscribe to the SNS alert topic. Leave blank to skip."
  type        = string
  default     = ""
}
