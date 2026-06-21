# Running the logs benchmark suite on a fresh VDS

Step-by-step guide for setting up a bare Ubuntu VDS to run `otelbench logs suite`.

## 0. (Optional) Provision the VDS with cherryctl

If you're using [Cherry Servers](https://www.cherryservers.com/), install [`cherryctl`](https://github.com/cherryservers/cherryctl) locally and provision a fresh Ubuntu server from your machine instead of clicking through the dashboard.

Install:

```bash
go install github.com/cherryservers/cherryctl/cmd/cherryctl@latest
```

Initialize a context (prompts for API token, Team ID, Project ID — generate the token in the Cherry Servers portal under *Account settings → API keys*):

```bash
cherryctl init --context bench
```

List plans/regions/images to pick slugs. This stack runs ClickHouse + oteldb + chotel + otelcol + Tempo on a single node, so prioritize NVMe capacity and RAM (for `--size large` runs and repeats) over raw core count — beyond ~16-32 cores you're not buying much for this workload. Filter for plans with a decent core count and list cores/RAM/storage/price/region stock as TSV to compare:

```bash
cherryctl plan list --output json | jq -r '
  .[] | select(.specs.cpus.cores >= 16)
  | [
      .slug,
      .specs.cpus.cores,
      .specs.memory.total,
      (.specs.storage | map(.name) | join("+")),
      (.pricing[] | select(.unit=="Hourly") | .price),
      (.available_regions | map(select(.stock_qty > 0) | .slug) | join(","))
    ]
  | @tsv' | sort -t$'\t' -k2,2n
```

At ~$2-3/hr, `amd-threadripper-pro-7975wx` (32 cores, 768GB RAM, NVMe 1TB+4TB, $2.50/hr) is a good pick — plenty of NVMe/RAM headroom for the `large` dataset tier plus repeat runs, and it has the best stock spread across regions (LT-Siauliai, NL-Amsterdam, US-Chicago, DE-Frankfurt, SE-Stockholm) so provisioning won't fail on a busy day.

Create the server (swap `<project_id>` for yours, and the region/image if you picked something else):

```bash
cherryctl server create \
  -p <project_id> \
  --plan amd-threadripper-pro-7975wx \
  --region LT-Siauliai \
  --image ubuntu_24_04_64bit \
  --hostname otelbench-vds
```

Get its public IP once it's done provisioning:

```bash
cherryctl server list
```

SSH in and continue with step 1 below:

```bash
ssh root@<server-ip>
```

When you're done benchmarking, tear it down to stop billing:

```bash
cherryctl server delete -i <server_id>
```

## 1. Update the system

```bash
sudo apt-get update && sudo apt-get upgrade -y
```

## 2. Install Docker + Compose plugin

```bash
sudo apt-get install -y ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

Allow your user to run docker without `sudo` (log out/in or `newgrp docker` to apply):

```bash
sudo usermod -aG docker "$USER"
newgrp docker
```

Verify:

```bash
docker run --rm hello-world
docker compose version
```

## 3. Install Go

The repo requires Go 1.26+ (see `go.mod`). Install the latest toolchain:

```bash
curl -fsSL https://go.dev/dl/go1.26.1.linux-amd64.tar.gz -o /tmp/go.tar.gz
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf /tmp/go.tar.gz
echo 'export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin' >> ~/.bashrc
source ~/.bashrc
go version
```

## 4. Install git and clone the repo

```bash
sudo apt-get install -y git
git clone https://github.com/oteldb/oteldb.git
cd oteldb
```

## 5. Build otelbench

```bash
go build -o ./otelbench ./cmd/otelbench
```

## 6. Run the benchmark suite

```bash
./otelbench logs suite --size small
```

This will:
1. Create a run directory under `./results/<timestamp>-<git-sha>/`
2. Download the Loghub dataset to `~/.cache/otelbench/loghub` (first run only; cached afterwards)
3. Bring up ClickHouse, oteldb, chotel, otelcol, and Tempo via `docker compose`
4. Ingest the dataset into oteldb over OTLP
5. Run the embedded LogQL query suite against Loki API
6. Tear the stack down (`docker compose down -v`)

Use `--size medium` or `--size large` for bigger datasets (more disk space and download time required). Use `--keep` to leave containers running for inspection, and `--no-up` to reuse a stack you started manually.

## 7. Render the Markdown report

```bash
./otelbench logs report ./results/<run-dir> -o report.md
```

## Notes for a fresh VDS

- Make sure outbound HTTPS (443) is allowed — the dataset download from Zenodo and Docker image pulls both need it.
- Reserve a few GB of free disk: Docker images (ClickHouse, otelcol, Tempo) plus the Loghub dataset (`large` tier is the biggest) plus ClickHouse data files.
- If testing local oteldb changes rather than the published image, build and tag a local image first, then pass `--oteldb-image=<your-tag>` (and `--chotel-image` if relevant) to `logs suite`.
