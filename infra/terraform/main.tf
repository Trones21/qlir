terraform {
  required_version = ">= 1.3.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
    http = {
      source  = "hashicorp/http"
      version = ">= 3.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "current" {}

# --- Ubuntu 24.04 LTS AMI (Canonical's public SSM parameter; stays current per-region) ---
data "aws_ssm_parameter" "ubuntu_2404" {
  name = "/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id"
}

# --- Default VPC (keeps this stack a one-command spin-up; no custom networking) ---
data "aws_vpc" "default" {
  default = true
}

# --- Auto-detect this machine's public IP for SSH lockdown (used when ssh_ingress_cidr is empty) ---
data "http" "myip" {
  url = "https://checkip.amazonaws.com"
}

locals {
  ssh_cidr      = var.ssh_ingress_cidr != "" ? var.ssh_ingress_cidr : "${chomp(data.http.myip.response_body)}/32"
  want_profile  = var.mode == "start"
  secret_arn    = "arn:aws:secretsmanager:${var.aws_region}:${data.aws_caller_identity.current.account_id}:secret:${var.secret_name}-*"
  user_data = var.mode == "none" ? null : templatefile("${path.module}/user_data.sh.tftpl", {
    repo_url    = var.repo_url
    mode        = var.mode
    secret_name = var.secret_name
    aws_region  = var.aws_region
  })
}

# --- Security group: SSH inbound only. The pipeline is OUTBOUND-only (Binance + Telegram),
#     and alerts are local files, so no application ports need to be exposed. ---
resource "aws_security_group" "qlir" {
  name        = "${var.name}-sg"
  description = "QLIR: SSH in only; all egress"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [local.ssh_cidr]
  }

  egress {
    description = "All outbound (Binance API, Telegram, package installs)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.name}-sg" }
}

# --- IAM role granting the instance read access to ONLY the pipeline's secret (mode = "start") ---
resource "aws_iam_role" "qlir" {
  count = local.want_profile ? 1 : 0
  name  = "${var.name}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "qlir_secret_read" {
  count = local.want_profile ? 1 : 0
  name  = "${var.name}-secret-read"
  role  = aws_iam_role.qlir[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "secretsmanager:GetSecretValue"
      Resource = local.secret_arn
    }]
  })
}

resource "aws_iam_instance_profile" "qlir" {
  count = local.want_profile ? 1 : 0
  name  = "${var.name}-profile"
  role  = aws_iam_role.qlir[0].name
}

resource "aws_instance" "qlir" {
  ami                    = data.aws_ssm_parameter.ubuntu_2404.value
  instance_type          = var.instance_type
  key_name               = var.key_name
  vpc_security_group_ids = [aws_security_group.qlir.id]
  iam_instance_profile   = local.want_profile ? aws_iam_instance_profile.qlir[0].name : null
  user_data              = local.user_data

  root_block_device {
    volume_size = var.root_volume_gb
    volume_type = "gp3"
    encrypted   = true
  }

  tags = {
    Name    = var.name
    Project = "qlir"
  }

  lifecycle {
    precondition {
      condition     = var.mode != "start" || var.secret_name != ""
      error_message = "mode = \"start\" requires secret_name (the Secrets Manager secret holding your env vars)."
    }
  }
}
