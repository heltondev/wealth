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

output "api_gateway_url" {
  description = "API Gateway invoke URL."
  value       = aws_api_gateway_stage.prod.invoke_url
}

output "cloudfront_distribution_id" {
  description = "CloudFront distribution ID for cache invalidation."
  value       = aws_cloudfront_distribution.main.id
}

output "cloudfront_domain_name" {
  description = "CloudFront distribution domain name."
  value       = aws_cloudfront_distribution.main.domain_name
}

output "frontend_bucket" {
  description = "S3 bucket for frontend static files."
  value       = aws_s3_bucket.frontend.id
}

output "data_bucket" {
  description = "S3 bucket for application data (reports, PDFs)."
  value       = aws_s3_bucket.data.id
}

output "lambda_function_name" {
  description = "Lambda function name."
  value       = aws_lambda_function.api.function_name
}

output "dynamodb_table_name" {
  description = "DynamoDB table name."
  value       = aws_dynamodb_table.main.name
}

output "lambda_artifacts_bucket" {
  description = "S3 bucket for Lambda deployment artifacts."
  value       = aws_s3_bucket.lambda_artifacts.id
}

output "github_actions_role_arn" {
  description = "IAM role ARN for GitHub Actions OIDC (null when disabled)."
  value       = try(aws_iam_role.github_actions[0].arn, null)
}
