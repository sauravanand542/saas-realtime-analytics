output "database_name" {
  value = snowflake_database.analytics.name
}

output "warehouse_name" {
  value = snowflake_warehouse.analytics.name
}

output "roles" {
  value = local.roles
}
