resource "aws_ssm_parameter" "api_base_url" {
  name  = "/${var.project}/API_BASE_URL"
  type  = "String"
  value = var.api_base_url
}

resource "aws_ssm_parameter" "kafka_bootstrap_servers" {
  name  = "/${var.project}/KAFKA_BOOTSTRAP_SERVERS"
  type  = "String"
  value = aws_msk_cluster.main.bootstrap_brokers
}

resource "aws_ssm_parameter" "redis_url" {
  name  = "/${var.project}/REDIS_URL"
  type  = "String"
  value = "redis://${aws_elasticache_cluster.redis.cache_nodes[0].address}:6379"
}

resource "aws_ssm_parameter" "sns_alert_topic_arn" {
  name  = "/${var.project}/SNS_ALERT_TOPIC_ARN"
  type  = "String"
  value = aws_sns_topic.alerts.arn
}

# Shared outputs for env_monitor_dashboard to read
resource "aws_ssm_parameter" "redis_url_shared" {
  name  = "/env-monitor-shared/REDIS_URL"
  type  = "String"
  value = "redis://${aws_elasticache_cluster.redis.cache_nodes[0].address}:6379"
}
