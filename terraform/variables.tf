variable "aws_region" {
  description = "AWS region for Cognito resources."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name prefix used in Cognito resource names."
  type        = string
  default     = "wealthhub"
}

variable "environment" {
  description = "Environment name used for tags."
  type        = string
  default     = "Production"
}

variable "owner_email" {
  description = "Primary admin email to create and add to ADMIN group."
  type        = string
  default     = "holiver.usa@gmail.com"
}

variable "google_client_id" {
  description = "Google OAuth client ID used by Cognito Google identity provider."
  type        = string
  sensitive   = true

  validation {
    condition     = length(trimspace(var.google_client_id)) > 0
    error_message = "google_client_id must be a non-empty Google OAuth Client ID."
  }
}

variable "google_client_secret" {
  description = "Google OAuth client secret used by Cognito Google identity provider."
  type        = string
  sensitive   = true

  validation {
    condition     = length(trimspace(var.google_client_secret)) > 0
    error_message = "google_client_secret must be a non-empty Google OAuth Client Secret."
  }
}

variable "cognito_domain_prefix" {
  description = "Cognito Hosted UI domain prefix (must be globally unique per AWS region)."
  type        = string
  default     = "wealthhub-auth"
}

variable "callback_urls" {
  description = "Allowed OAuth callback URLs for Cognito app client."
  type        = list(string)
  default = [
    "http://localhost:5173/dashboard",
    "https://invest.oliverapp.net/dashboard",
    "https://investiments.oliverapp.net/dashboard"
  ]
}

variable "logout_urls" {
  description = "Allowed logout redirect URLs for Cognito app client."
  type        = list(string)
  default = [
    "http://localhost:5173/login",
    "https://invest.oliverapp.net/login",
    "https://investiments.oliverapp.net/login"
  ]
}

variable "create_initial_admin_user" {
  description = "Whether to create an initial Cognito user and assign ADMIN group."
  type        = bool
  default     = true
}

variable "create_api_gateway_authorizer" {
  description = "Whether to create a Cognito authorizer on an existing API Gateway REST API."
  type        = bool
  default     = false
}

variable "api_gateway_rest_api_id" {
  description = "Existing API Gateway REST API ID where the Cognito authorizer will be created."
  type        = string
  default     = ""
}

variable "api_gateway_authorizer_name" {
  description = "Optional custom name for the API Gateway Cognito authorizer."
  type        = string
  default     = ""
}

locals {
  common_tags = {
    Project     = "WealthHub"
    Environment = var.environment
    OwnerEmail  = var.owner_email
    ManagedBy   = "Terraform"
  }
}
