output "cognito_user_pool_id" {
  description = "Cognito user pool ID."
  value       = aws_cognito_user_pool.main.id
}

output "cognito_user_pool_arn" {
  description = "Cognito user pool ARN."
  value       = aws_cognito_user_pool.main.arn
}

output "cognito_user_pool_client_id" {
  description = "Cognito app client ID for frontend."
  value       = aws_cognito_user_pool_client.client.id
}

output "cognito_hosted_ui_domain_prefix" {
  description = "Cognito Hosted UI domain prefix."
  value       = aws_cognito_user_pool_domain.main.domain
}

output "cognito_hosted_ui_domain" {
  description = "Cognito Hosted UI domain for frontend VITE_COGNITO_DOMAIN."
  value       = "${aws_cognito_user_pool_domain.main.domain}.auth.${var.aws_region}.amazoncognito.com"
}

output "cognito_hosted_ui_url" {
  description = "Cognito Hosted UI base URL."
  value       = "https://${aws_cognito_user_pool_domain.main.domain}.auth.${var.aws_region}.amazoncognito.com"
}

output "frontend_auth_env" {
  description = "Frontend auth environment values to copy into frontend/.env."
  value = {
    VITE_USER_POOL_ID      = aws_cognito_user_pool.main.id
    VITE_APP_CLIENT_ID     = aws_cognito_user_pool_client.client.id
    VITE_COGNITO_DOMAIN    = "${aws_cognito_user_pool_domain.main.domain}.auth.${var.aws_region}.amazoncognito.com"
    VITE_REDIRECT_SIGN_IN  = try(var.callback_urls[0], "")
    VITE_REDIRECT_SIGN_OUT = try(var.logout_urls[0], "")
  }
}

output "api_gateway_cognito_authorizer_id" {
  description = "Created API Gateway Cognito authorizer ID (null when disabled)."
  value       = try(aws_api_gateway_authorizer.cognito[0].id, null)
}
