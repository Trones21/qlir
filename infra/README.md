# QLIR Infrastructure (AWS)

Infrastructure-as-code to spin up a single EC2 host to run the QLIR pipeline. Two equivalent
options are provided — use whichever fits your workflow:

- **[terraform/](terraform/)** — HashiCorp Terraform
- **[cloudformation/](cloudformation/)** — AWS CloudFormation

Both create the same thing: one Ubuntu 24.04 EC2 instance, a security group that allows **SSH
inbound only**, and an encrypted `gp3` root volume — and both support three bootstrap **modes**.

---

## ⚠️ Region requirement: must be OUTSIDE the United States

**Binance blocks US IP addresses.** The data server calls the Binance API, so the instance
**must run in a non-US region.** **Europe is recommended** (e.g. `eu-central-1` Frankfurt,
`eu-west-1` Ireland, `eu-north-1` Stockholm). The availability zone does not matter.

- **Terraform** enforces this: `aws_region` rejects any `us-*` region with a validation error.
- **CloudFormation** cannot self-enforce the deploy region — **you** must pass a non-US region
  via `--region` when you deploy.

---

## Bootstrap modes

| Mode | What happens on first boot |
|---|---|
| `none` | Bare instance. No user-data — you do all setup manually. |
| `install` *(default)* | Installs system deps (via the repo's [install_system_deps.sh](../src/qlir/servers/install_system_deps.sh)), clones the repo, runs `poetry install`. **Does not start services.** |
| `start` | Everything in `install`, then **pulls secrets from AWS Secrets Manager**, writes them to `~/set_telegram_env_vars.sh`, and runs `start_all_simple.sh`. Requires a secret (below) and attaches a **scoped IAM role**. |

### `start` mode: the secret

Create a Secrets Manager secret (out of band — never in IaC/state) whose value is a JSON object
of the env vars the pipeline needs:

```bash
aws secretsmanager create-secret \
  --region eu-central-1 \
  --name qlir/telegram \
  --secret-string '{
    "OPS_TELEGRAM_BOT_TOKEN":"...",
    "DATA_PIPELINE_TELEGRAM_BOT_TOKEN":"...",
    "TRADABLE_HUMAN_TELEGRAM_BOT_TOKEN":"...",
    "POSITIONING_TELEGRAM_BOT_TOKEN":"...",
    "TELEGRAM_CHAT_ID":"..."
  }'
```

The bootstrap turns each JSON key into `export KEY="VALUE"` and appends
`QLIR_ALERTS_DIR`/`QLIR_DATA_ROOT`, then `start_all_simple.sh` sources that file. The instance's
IAM role grants `secretsmanager:GetSecretValue` on **only** this secret's ARN. If you encrypt
the secret with a **customer-managed** KMS key (not the default `aws/secretsmanager`), also add
`kms:Decrypt` on that key to the role.

---

## Prerequisites

- AWS credentials configured locally (`aws configure` / environment / SSO).
- An **existing EC2 key pair** in the target region (neither stack creates keys, to keep private
  keys out of state).
- Terraform ≥ 1.3 (Terraform option) or the AWS CLI (CloudFormation option).

## What gets created

| Resource | Notes |
|---|---|
| EC2 instance | Ubuntu 24.04 LTS (AMI from Canonical's public SSM parameter), `t3.medium` default |
| Security group | **SSH (22) inbound only**; all egress. Pipeline is outbound-only, so no app ports are exposed. |
| Root volume | 50 GiB `gp3`, encrypted |
| IAM role + instance profile | **only in `start` mode**, scoped to read the one secret |

---

## Terraform

SSH is locked to **your current public IP** automatically (auto-detected via
`checkip.amazonaws.com`) unless you set `ssh_ingress_cidr`.

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars   # edit: key_name, aws_region, mode, (secret_name)
terraform init
terraform apply
# read the "summary" output — it prints exactly how the host is wired up

terraform destroy   # tear down when done
```

Key variables (see [variables.tf](terraform/variables.tf)): `key_name` (required),
`aws_region` (non-US), `mode` (`none`/`install`/`start`), `secret_name` (required for `start`),
`ssh_ingress_cidr` (empty = auto-detect your IP), `instance_type`, `root_volume_gb`, `repo_url`.

## CloudFormation

CloudFormation can't auto-detect your IP, so pass it (the `curl` trick fills it in):

```bash
aws cloudformation deploy \
  --region eu-central-1 \                       # MUST be non-US
  --template-file infra/cloudformation/qlir-ec2.yaml \
  --stack-name qlir \
  --capabilities CAPABILITY_IAM \               # needed: start mode creates an IAM role
  --parameter-overrides \
      KeyName=my-eu-keypair \
      VpcId=vpc-xxxxxxxx \
      SubnetId=subnet-xxxxxxxx \
      SshIngressCidr="$(curl -s https://checkip.amazonaws.com)/32" \
      Mode=start \
      SecretName=qlir/telegram

# view outputs (public IP/DNS, ssh command, bootstrap-watch command)
aws cloudformation describe-stacks --region eu-central-1 --stack-name qlir \
  --query 'Stacks[0].Outputs'

aws cloudformation delete-stack --region eu-central-1 --stack-name qlir
```

`--capabilities CAPABILITY_IAM` is only strictly required for `Mode=start` (which creates the
role), but it's harmless to always pass. `VpcId`/`SubnetId` can be your default VPC and a public
subnet in it.

---

## Where the logging is (know exactly how it's set up)

- **After `apply`/`deploy`:** Terraform prints a full **`summary`** output (region, instance,
  public IP/DNS, SSH CIDR, mode, secret, IAM role, next steps). CloudFormation exposes the same
  facts as stack **Outputs** (including a ready-to-run `WatchBootstrap` command).
- **On the instance**, the bootstrap logs verbosely (every command + output) to
  **`/var/log/qlir-bootstrap.log`**, and cloud-init also captures it in
  `/var/log/cloud-init-output.log`. It ends with a banner summarizing mode, whether secrets
  loaded, and whether services started:

```bash
ssh ubuntu@<public-dns> 'sudo tail -f /var/log/qlir-bootstrap.log'
```

## After it's up

```bash
ssh ubuntu@<public-dns>          # from the outputs

sudo -u ubuntu tmux ls            # running services (start mode)
tail -f ~/qlir/src/qlir/servers/logs/*.log

# install mode: set your secrets, then start manually
cd ~/qlir/src/qlir/servers && ./start_all_simple.sh
```

> **Secrets:** `start` mode pulls them from Secrets Manager into `~/set_telegram_env_vars.sh`.
> In `install`/`none` mode, create that file yourself (it's what `start_all_simple.sh` sources).
