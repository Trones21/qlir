output "instance_id" {
  description = "EC2 instance ID."
  value       = aws_instance.qlir.id
}

output "public_ip" {
  description = "Public IPv4 address."
  value       = aws_instance.qlir.public_ip
}

output "public_dns" {
  description = "Public DNS name."
  value       = aws_instance.qlir.public_dns
}

output "region" {
  description = "Region the instance runs in (must be non-US for Binance)."
  value       = var.aws_region
}

output "ssh" {
  description = "Convenience SSH command (Ubuntu AMIs use the 'ubuntu' user)."
  value       = "ssh ubuntu@${aws_instance.qlir.public_dns}"
}

output "ssh_allowed_from" {
  description = "CIDR permitted to SSH (auto-detected from your IP unless overridden)."
  value       = local.ssh_cidr
}

# A single human-readable summary of exactly how the host is wired up.
output "summary" {
  description = "Full setup summary — read this after apply."
  value       = <<-EOT

    ============================================================
     QLIR EC2 — provisioned
    ============================================================
     region            : ${var.aws_region}  (non-US, required for Binance)
     instance          : ${aws_instance.qlir.id}  (${var.instance_type})
     ami               : Ubuntu 24.04 LTS (${data.aws_ssm_parameter.ubuntu_2404.value})
     public ip / dns   : ${aws_instance.qlir.public_ip}  /  ${aws_instance.qlir.public_dns}
     root volume       : ${var.root_volume_gb} GiB gp3 (encrypted)
     ssh allowed from  : ${local.ssh_cidr}
     bootstrap mode    : ${var.mode}
     secret (start)    : ${var.mode == "start" ? var.secret_name : "n/a (not start mode)"}
     iam role          : ${local.want_profile ? "${var.name}-role (secretsmanager:GetSecretValue on ${var.secret_name})" : "none"}
    ------------------------------------------------------------
     Connect:
       ssh ubuntu@${aws_instance.qlir.public_dns}

     Watch the bootstrap (installs/starts happen on first boot):
       ssh ubuntu@${aws_instance.qlir.public_dns} 'sudo tail -f /var/log/qlir-bootstrap.log'
       # cloud-init also logs to /var/log/cloud-init-output.log

     Once up:
       tmux ls                                  # running services (start mode)
       tail -f ~/qlir/src/qlir/servers/logs/*.log
    ${var.mode == "install" ? "   # mode=install: set your secrets, then: cd ~/qlir/src/qlir/servers && ./start_all_simple.sh" : ""}
    ${var.mode == "none" ? "   # mode=none: bare box. Run infra + install yourself (see infra/README.md)." : ""}
    ============================================================
  EOT
}
