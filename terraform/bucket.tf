resource "aws_s3_bucket" "bucket" {
  bucket = "terraform-project-glue-cicd"
}

resource "aws_s3_bucket" "artifacts" {
  bucket = var.artifacts_bucket_name

  tags = {
    Name        = var.artifacts_bucket_name
    Environment = var.environment
    Project     = "GlueETL"
  }
}

resource "aws_s3_bucket_versioning" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

