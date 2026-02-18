resource "aws_cognito_user_pool" "main" {
  name = "${var.project_name}-user-pool"

  # Keep account creation controlled by admins for local users.
  # Federated Google users still authenticate through Hosted UI.
  admin_create_user_config {
    allow_admin_create_user_only = true
  }

  auto_verified_attributes = ["email"]
  username_attributes      = ["email"]

  password_policy {
    minimum_length    = 8
    require_lowercase = true
    require_numbers   = true
    require_symbols   = true
    require_uppercase = true
  }

  tags = local.common_tags
}

resource "aws_cognito_user_pool_domain" "main" {
  domain       = var.cognito_domain_prefix
  user_pool_id = aws_cognito_user_pool.main.id
}

resource "aws_cognito_identity_provider" "google" {
  user_pool_id  = aws_cognito_user_pool.main.id
  provider_name = "Google"
  provider_type = "Google"

  provider_details = {
    authorize_scopes     = "email profile openid"
    client_id            = var.google_client_id
    client_secret        = var.google_client_secret
    attributes_url       = "https://people.googleapis.com/v1/people/me?personFields="
    authorize_url        = "https://accounts.google.com/o/oauth2/v2/auth"
    oidc_issuer          = "https://accounts.google.com"
    token_request_method = "POST"
    token_url            = "https://www.googleapis.com/oauth2/v4/token"
  }

  attribute_mapping = {
    email    = "email"
    username = "sub"
    name     = "name"
  }
}

resource "aws_cognito_user_pool_client" "client" {
  name         = "${var.project_name}-app-client"
  user_pool_id = aws_cognito_user_pool.main.id

  generate_secret = false

  supported_identity_providers = ["COGNITO", "Google"]

  allowed_oauth_flows_user_pool_client = true
  allowed_oauth_flows                  = ["code"]
  allowed_oauth_scopes                 = ["email", "openid", "profile"]

  callback_urls = var.callback_urls
  logout_urls   = var.logout_urls

  prevent_user_existence_errors = "ENABLED"

  depends_on = [aws_cognito_identity_provider.google]
}

resource "aws_cognito_user_group" "admin" {
  user_pool_id = aws_cognito_user_pool.main.id
  name         = "ADMIN"
  description  = "Application administrators with full access."
  precedence   = 1
}

resource "aws_cognito_user_group" "editor" {
  user_pool_id = aws_cognito_user_pool.main.id
  name         = "EDITOR"
  description  = "Editors with create/update permissions."
  precedence   = 2
}

resource "aws_cognito_user_group" "viewer" {
  user_pool_id = aws_cognito_user_pool.main.id
  name         = "VIEWER"
  description  = "Read-only access."
  precedence   = 3
}

resource "aws_cognito_user" "admin" {
  count = var.create_initial_admin_user ? 1 : 0

  user_pool_id = aws_cognito_user_pool.main.id
  username     = var.owner_email

  attributes = {
    email          = var.owner_email
    email_verified = "true"
  }

  lifecycle {
    ignore_changes = [attributes]
  }
}

resource "aws_cognito_user_in_group" "admin_membership" {
  count = var.create_initial_admin_user ? 1 : 0

  user_pool_id = aws_cognito_user_pool.main.id
  group_name   = aws_cognito_user_group.admin.name
  username     = aws_cognito_user.admin[0].username
}
