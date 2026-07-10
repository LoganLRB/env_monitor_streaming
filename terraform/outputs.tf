output "kafka_bootstrap_servers" {
  description = "MSK Kafka bootstrap servers"
  value       = aws_msk_cluster.main.bootstrap_brokers
}

output "redis_endpoint" {
  description = "ElastiCache Redis endpoint"
  value       = aws_elasticache_cluster.redis.cache_nodes[0].address
}

output "sns_alert_topic_arn" {
  description = "SNS topic ARN for wildfire alerts"
  value       = aws_sns_topic.alerts.arn
}

output "ecr_repository_url" {
  description = "ECR repository URL for the streaming Docker image"
  value       = aws_ecr_repository.main.repository_url
}

output "ecs_cluster_name" {
  description = "ECS cluster name"
  value       = aws_ecs_cluster.main.name
}

output "ecs_service_name" {
  description = "ECS service name"
  value       = aws_ecs_service.streaming.name
}
