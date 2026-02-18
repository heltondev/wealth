resource "aws_api_gateway_authorizer" "cognito" {
  count = var.create_api_gateway_authorizer ? 1 : 0

  name = trimspace(var.api_gateway_authorizer_name) != "" ? var.api_gateway_authorizer_name : "${var.project_name}-cognito-authorizer"

  rest_api_id     = var.api_gateway_rest_api_id
  type            = "COGNITO_USER_POOLS"
  provider_arns   = [aws_cognito_user_pool.main.arn]
  identity_source = "method.request.header.Authorization"
}
