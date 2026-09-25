# Custom roles are granted to SYSADMIN so an admin can see and own the objects
# those roles create. This is the usual Snowflake role-hierarchy pattern.

resource "snowflake_grant_account_role" "loader_to_sysadmin" {
  role_name        = snowflake_account_role.loader.name
  parent_role_name = "SYSADMIN"
}

resource "snowflake_grant_account_role" "transformer_to_sysadmin" {
  role_name        = snowflake_account_role.transformer.name
  parent_role_name = "SYSADMIN"
}

resource "snowflake_grant_account_role" "reporter_to_sysadmin" {
  role_name        = snowflake_account_role.reporter.name
  parent_role_name = "SYSADMIN"
}

resource "snowflake_grant_account_role" "user_loader" {
  count     = var.grant_to_user == "" ? 0 : 1
  role_name = snowflake_account_role.loader.name
  user_name = var.grant_to_user
}

resource "snowflake_grant_account_role" "user_transformer" {
  count     = var.grant_to_user == "" ? 0 : 1
  role_name = snowflake_account_role.transformer.name
  user_name = var.grant_to_user
}

resource "snowflake_grant_account_role" "user_reporter" {
  count     = var.grant_to_user == "" ? 0 : 1
  role_name = snowflake_account_role.reporter.name
  user_name = var.grant_to_user
}

locals {
  roles = {
    loader      = snowflake_account_role.loader.name
    transformer = snowflake_account_role.transformer.name
    reporter    = snowflake_account_role.reporter.name
  }
  transformer_schemas = toset(["staging", "intermediate", "marts", "snapshots"])
}

resource "snowflake_grant_privileges_to_account_role" "warehouse_usage" {
  for_each          = local.roles
  privileges        = ["USAGE"]
  account_role_name = each.value
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.analytics.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "database_usage" {
  for_each          = local.roles
  privileges        = ["USAGE"]
  account_role_name = each.value
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.analytics.name
  }
}

# dbt issues CREATE SCHEMA IF NOT EXISTS. The schemas are still declared above
# so the layout is reviewed here. The loader does not get this privilege.
resource "snowflake_grant_privileges_to_account_role" "transformer_create_schema" {
  privileges        = ["CREATE SCHEMA"]
  account_role_name = snowflake_account_role.transformer.name
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.analytics.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "loader_raw_schema" {
  privileges        = ["USAGE", "CREATE TABLE"]
  account_role_name = snowflake_account_role.loader.name
  on_schema {
    schema_name = snowflake_schema.this["raw"].fully_qualified_name
  }
}

resource "snowflake_grant_privileges_to_account_role" "loader_raw_tables" {
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"]
  account_role_name = snowflake_account_role.loader.name
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = snowflake_schema.this["raw"].fully_qualified_name
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "loader_raw_future_tables" {
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"]
  account_role_name = snowflake_account_role.loader.name
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = snowflake_schema.this["raw"].fully_qualified_name
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_raw_schema" {
  privileges        = ["USAGE"]
  account_role_name = snowflake_account_role.transformer.name
  on_schema {
    schema_name = snowflake_schema.this["raw"].fully_qualified_name
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_raw_tables" {
  privileges        = ["SELECT"]
  account_role_name = snowflake_account_role.transformer.name
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = snowflake_schema.this["raw"].fully_qualified_name
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_raw_future_tables" {
  privileges        = ["SELECT"]
  account_role_name = snowflake_account_role.transformer.name
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = snowflake_schema.this["raw"].fully_qualified_name
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_schema" {
  for_each          = local.transformer_schemas
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
  account_role_name = snowflake_account_role.transformer.name
  on_schema {
    schema_name = snowflake_schema.this[each.key].fully_qualified_name
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_tables" {
  for_each          = local.transformer_schemas
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"]
  account_role_name = snowflake_account_role.transformer.name
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = snowflake_schema.this[each.key].fully_qualified_name
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "reporter_marts_schema" {
  privileges        = ["USAGE"]
  account_role_name = snowflake_account_role.reporter.name
  on_schema {
    schema_name = snowflake_schema.this["marts"].fully_qualified_name
  }
}

resource "snowflake_grant_privileges_to_account_role" "reporter_marts_tables" {
  privileges        = ["SELECT"]
  account_role_name = snowflake_account_role.reporter.name
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = snowflake_schema.this["marts"].fully_qualified_name
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "reporter_marts_views" {
  privileges        = ["SELECT"]
  account_role_name = snowflake_account_role.reporter.name
  on_schema_object {
    future {
      object_type_plural = "VIEWS"
      in_schema          = snowflake_schema.this["marts"].fully_qualified_name
    }
  }
}
