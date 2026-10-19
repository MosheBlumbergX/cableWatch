# ---------------------------------------------------------------------------
# AWS hosting: one small Graviton instance runs the AIS producer and the
# dashboard as systemd services (and loads the cable geography once); CloudFront gives it a public HTTPS URL. The app is
# shipped as a zip in S3 (no container build), and Kafka credentials come
# from Secrets Manager.
# ---------------------------------------------------------------------------

provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile

  default_tags {
    tags = local.aws_tags
  }
}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

data "aws_ssm_parameter" "al2023_arm64" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
}

data "aws_ec2_managed_prefix_list" "cloudfront" {
  name = "com.amazonaws.global.cloudfront.origin-facing"
}

resource "random_id" "suffix" {
  byte_length = 4
}

# ---------------------------------------------------------------------------
# App bundle
# ---------------------------------------------------------------------------
data "archive_file" "app" {
  type        = "zip"
  source_dir  = "${path.module}/../app"
  output_path = "${path.module}/.build/app.zip"
  excludes    = [".venv", ".venv/**", ".env.app", "__pycache__", "__pycache__/**", ".cache", ".cache/**"]
}

resource "aws_s3_bucket" "app" {
  bucket        = "cablewatch-app-${random_id.suffix.hex}"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "app" {
  bucket                  = aws_s3_bucket.app.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_object" "app" {
  bucket = aws_s3_bucket.app.id
  key    = "app-${data.archive_file.app.output_md5}.zip"
  source = data.archive_file.app.output_path
  etag   = data.archive_file.app.output_md5
}

# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------
resource "aws_secretsmanager_secret" "kafka" {
  name                    = "cablewatch/confluent-${random_id.suffix.hex}"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "kafka" {
  secret_id = aws_secretsmanager_secret.kafka.id
  secret_string = jsonencode({
    BOOTSTRAP_SERVERS          = replace(confluent_kafka_cluster.cw.bootstrap_endpoint, "SASL_SSL://", "")
    KAFKA_API_KEY              = confluent_api_key.kafka.id
    KAFKA_API_SECRET           = confluent_api_key.kafka.secret
    SCHEMA_REGISTRY_URL        = data.confluent_schema_registry_cluster.sr.rest_endpoint
    SCHEMA_REGISTRY_API_KEY    = confluent_api_key.schema_registry.id
    SCHEMA_REGISTRY_API_SECRET = confluent_api_key.schema_registry.secret
    AISSTREAM_API_KEY          = var.aisstream_api_key
    CONSOLE_URL                = "https://confluent.cloud/environments/${confluent_environment.cw.id}/flink/statements"
    LINEAGE_URL                = "https://confluent.cloud/environments/${confluent_environment.cw.id}/clusters/${confluent_kafka_cluster.cw.id}/stream-lineage/stream/topic/incidents"
  })
}

# ---------------------------------------------------------------------------
# Instance role: read the bundle and the secret; SSM for shell access (no SSH)
# ---------------------------------------------------------------------------
resource "aws_iam_role" "app" {
  name = "cablewatch-app-${random_id.suffix.hex}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "app" {
  role = aws_iam_role.app.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "s3:GetObject"
        Resource = "${aws_s3_bucket.app.arn}/*"
      },
      {
        Effect   = "Allow"
        Action   = "secretsmanager:GetSecretValue"
        Resource = aws_secretsmanager_secret.kafka.arn
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.app.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "app" {
  name = aws_iam_role.app.name
  role = aws_iam_role.app.name
}

# ---------------------------------------------------------------------------
# Network: only CloudFront can reach the dashboard port
# ---------------------------------------------------------------------------
resource "aws_security_group" "app" {
  name        = "cablewatch-app-${random_id.suffix.hex}"
  description = "Cable Watch dashboard, reachable from CloudFront only"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description     = "Dashboard from CloudFront"
    from_port       = 8000
    to_port         = 8000
    protocol        = "tcp"
    prefix_list_ids = [data.aws_ec2_managed_prefix_list.cloudfront.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ---------------------------------------------------------------------------
# Instance
# ---------------------------------------------------------------------------
resource "aws_instance" "app" {
  ami                    = data.aws_ssm_parameter.al2023_arm64.value
  instance_type          = var.aws_instance_type
  subnet_id              = data.aws_subnets.default.ids[0]
  vpc_security_group_ids = [aws_security_group.app.id]
  iam_instance_profile   = aws_iam_instance_profile.app.name

  metadata_options {
    http_tokens = "required"
  }

  root_block_device {
    volume_size = 20
    volume_type = "gp3"
    encrypted   = true
    tags        = local.aws_tags
  }

  # A new bundle (any app change) replaces the instance.
  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    region     = var.aws_region
    bucket     = aws_s3_bucket.app.id
    key        = aws_s3_object.app.key
    secret_arn = aws_secretsmanager_secret.kafka.arn
    repo_url   = var.repo_url
    ntfy_url   = "${var.ntfy_server}/${local.ntfy_topic}"
    cooldown   = var.replay_cooldown_minutes
    tableflow  = var.tableflow_enabled
    llm        = local.llm_enabled
  })
  user_data_replace_on_change = true

  tags = {
    Name = "cablewatch-app"
  }

  depends_on = [
    aws_secretsmanager_secret_version.kafka,
    aws_iam_role_policy.app,
    confluent_flink_statement.ddl,
  ]
}

# Stable origin hostname across instance replacements.
resource "aws_eip" "app" {
  domain   = "vpc"
  instance = aws_instance.app.id
}

# ---------------------------------------------------------------------------
# CloudFront: HTTPS URL; nothing cached except the cable routes and static assets
# ---------------------------------------------------------------------------
locals {
  aws_tags = merge({ Project = "cablewatch", ManagedBy = "terraform" }, var.aws_tags)

  cf_caching_disabled       = "4135ea2d-6df8-44a3-9df3-4b5a84be39ad"
  cf_caching_optimized      = "658327ea-f89d-4fab-a63d-7e88639e58f6"
  cf_all_viewer_except_host = "b689b0a8-53d0-40ab-baf2-68738e2966ac"
  origin_id                 = "cablewatch-dashboard"
}

resource "aws_cloudfront_distribution" "app" {
  enabled         = true
  comment         = "Cable Watch dashboard"
  price_class     = "PriceClass_100"
  is_ipv6_enabled = true

  origin {
    origin_id   = local.origin_id
    domain_name = aws_eip.app.public_dns
    custom_origin_config {
      http_port                = 8000
      https_port               = 443
      origin_protocol_policy   = "http-only"
      origin_ssl_protocols     = ["TLSv1.2"]
      origin_read_timeout      = 60 # the event stream sends a keepalive every 15 s
      origin_keepalive_timeout = 60
    }
  }

  default_cache_behavior {
    target_origin_id         = local.origin_id
    viewer_protocol_policy   = "redirect-to-https"
    allowed_methods          = ["GET", "HEAD", "OPTIONS"]
    cached_methods           = ["GET", "HEAD"]
    cache_policy_id          = local.cf_caching_disabled
    origin_request_policy_id = local.cf_all_viewer_except_host
    compress                 = false # compression would buffer the event stream
  }

  # Cable routes: the same for every visitor; the server sets a max-age.
  ordered_cache_behavior {
    path_pattern           = "/api/cables"
    target_origin_id       = local.origin_id
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD"]
    cached_methods         = ["GET", "HEAD"]
    cache_policy_id        = local.cf_caching_optimized
    compress               = true
  }

  ordered_cache_behavior {
    path_pattern           = "/static/*"
    target_origin_id       = local.origin_id
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD"]
    cached_methods         = ["GET", "HEAD"]
    cache_policy_id        = local.cf_caching_optimized
    compress               = true
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    cloudfront_default_certificate = true
  }
}
