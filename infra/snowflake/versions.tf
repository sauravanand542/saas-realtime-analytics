terraform {
  required_version = ">= 1.5.0"

  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = ">= 2.20.0, < 3.0.0"
    }
  }
}

provider "snowflake" {
  # Credentials are read from the environment, not from this file.
  # SNOWFLAKE_ORGANIZATION_NAME, SNOWFLAKE_ACCOUNT_NAME, SNOWFLAKE_USER,
  # SNOWFLAKE_PASSWORD, and SNOWFLAKE_ROLE. See .env.example.
}
