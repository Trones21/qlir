variable "aws_region" {
  description = <<-EOT
    AWS region to deploy into. MUST be outside the United States — Binance blocks
    US IP addresses, so the data server cannot run there. Europe is recommended.
    Availability zone does not matter.
  EOT
  type    = string
  default = "eu-central-1" # Frankfurt

  validation {
    condition     = !startswith(var.aws_region, "us-")
    error_message = "Binance blocks US IPs. Pick a non-US region (e.g. eu-central-1, eu-west-1, eu-north-1)."
  }
}

variable "name" {
  description = "Name prefix applied to created resources and tags."
  type        = string
  default     = "qlir"
}

variable "instance_type" {
  description = "EC2 instance type."
  type        = string
  default     = "t3.medium" # 2 vCPU / 4 GiB — fine for an MVP single-symbol pipeline
}

variable "key_name" {
  description = "Name of an EXISTING EC2 key pair to grant SSH access. Create one in the target region first."
  type        = string
}

variable "ssh_ingress_cidr" {
  description = <<-EOT
    CIDR allowed to SSH in. Leave empty (default) to auto-detect THIS machine's public
    IP and lock SSH to it (/32). Set explicitly to override, e.g. "203.0.113.4/32" or a
    range. Avoid "0.0.0.0/0".
  EOT
  type    = string
  default = "" # empty => auto-detect caller IP
}

variable "root_volume_gb" {
  description = "Root EBS volume size (GiB). 1s data + parquet grows; 50+ recommended."
  type        = number
  default     = 50
}

variable "mode" {
  description = <<-EOT
    Bootstrap behavior on first boot:
      "none"    - bare instance, no user-data (do everything manually).
      "install" - install system deps + clone + `poetry install`. Does NOT start services.
      "start"   - everything in "install", then pull secrets from Secrets Manager and start
                  the pipeline. Requires `secret_name` and attaches a scoped IAM role.
  EOT
  type    = string
  default = "install"

  validation {
    condition     = contains(["none", "install", "start"], var.mode)
    error_message = "mode must be one of: none, install, start."
  }
}

variable "secret_name" {
  description = <<-EOT
    AWS Secrets Manager secret NAME (not ARN) holding a JSON object of env vars to export
    before starting the pipeline, e.g.:
      {"OPS_TELEGRAM_BOT_TOKEN":"...","DATA_PIPELINE_TELEGRAM_BOT_TOKEN":"...",
       "TRADABLE_HUMAN_TELEGRAM_BOT_TOKEN":"...","POSITIONING_TELEGRAM_BOT_TOKEN":"...",
       "TELEGRAM_CHAT_ID":"..."}
    Only used (and required) when mode = "start". The instance gets read access to just
    this secret.
  EOT
  type    = string
  default = ""
}

variable "repo_url" {
  description = "Git URL cloned during bootstrap."
  type        = string
  default     = "https://github.com/Trones21/qlir.git"
}
