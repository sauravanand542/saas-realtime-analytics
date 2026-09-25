variable "database_name" {
  type        = string
  description = "Analytics database."
  default     = "ANALYTICS"
}

variable "warehouse_name" {
  type        = string
  description = "Transform warehouse. Starts suspended and auto-suspends."
  default     = "ANALYTICS_WH"
}

variable "loader_role_name" {
  type    = string
  default = "LOADER"
}

variable "transformer_role_name" {
  type    = string
  default = "TRANSFORMER"
}

variable "reporter_role_name" {
  type    = string
  default = "REPORTER"
}

variable "grant_to_user" {
  type        = string
  description = "Existing Snowflake user that should receive the three roles. Leave empty to skip."
  default     = ""
}

variable "auto_suspend_seconds" {
  type        = number
  description = "Idle seconds before the warehouse suspends. 60 is intentional for a trial."
  default     = 60
}
