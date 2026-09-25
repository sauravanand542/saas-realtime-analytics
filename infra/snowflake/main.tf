resource "snowflake_database" "analytics" {
  name    = var.database_name
  comment = "SaaS product analytics. Raw is the batch landing zone and the future CDC contract."
}

resource "snowflake_warehouse" "analytics" {
  name                = var.warehouse_name
  warehouse_size      = "XSMALL"
  auto_suspend        = var.auto_suspend_seconds
  auto_resume         = "true"
  initially_suspended = true
  comment             = "Single warehouse for load and transform. Suspended until a query arrives."
}

locals {
  schemas = {
    raw          = "Landing tables. Batch ingest writes here. A CDC consumer can use the same column contract."
    staging      = "dbt staging views."
    intermediate = "dbt intermediate models."
    marts        = "Published marts."
    snapshots    = "dbt snapshot tables."
  }
}

resource "snowflake_schema" "this" {
  for_each = local.schemas
  database = snowflake_database.analytics.name
  name     = upper(each.key)
  comment  = each.value
}

resource "snowflake_account_role" "loader" {
  name    = var.loader_role_name
  comment = "Loads raw tables. No access to marts."
}

resource "snowflake_account_role" "transformer" {
  name    = var.transformer_role_name
  comment = "dbt role. Reads raw and builds staging, intermediate, marts, and snapshots."
}

resource "snowflake_account_role" "reporter" {
  name    = var.reporter_role_name
  comment = "Read published marts only."
}
