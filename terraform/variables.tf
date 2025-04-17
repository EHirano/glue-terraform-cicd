variable "artifacts_bucket_name" {
  description = "S3 bucket to store Glue job dependencies"
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev, prd, etc.)"
  type        = string
}
