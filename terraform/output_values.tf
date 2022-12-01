output "frontend_url" {
  value       = "https://${tls_cert_request.default.dns_names[0]}"
  description = "The url to access CRMint UI."
}

output "region" {
  value       = var.region
  description = "Region used to deploy CRMint."
}

output "migrate_image" {
  value       = local.migrate_image
  description = "Docker image (with tag) for the controller service."
}

output "migrate_sql_conn_name" {
  value       = local.migrate_sql_conn_name
  description = "Database instance connection name."
}

output "cloud_db_uri" {
  value       = local.cloud_db_uri
  description = "Database connection URI."
  sensitive   = true
}

output "cloud_build_worker_pool" {
  value       = local.pool
  description = "Cloud Build worker pool."
}