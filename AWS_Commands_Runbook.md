# AWS Command Runbook — IST3134 Group Assignment
### eCommerce behavior data (2019-Nov.csv, ~9GB) on Amazon EMR + PySpark

This is written for **AWS Academy Learner Lab** (temporary credentials, `vockey` key pair,
`EMR_DefaultRole` / `EMR_EC2_DefaultRole`) — the same environment as Labs 1–9. Run these
yourself in your own Learner Lab session; I don't have access to your AWS account.

**Confirmed from your files:**
- Dataset: `C:\BSDA\Yr3 Sem 8\BIg Data\archive\2019-Nov.csv` — 9GB, schema `event_time,
  event_type, product_id, category_id, category_code, brand, price, user_id, user_session`.
- Cluster spec (matches Lab 5/6): 1 primary + 2 core nodes, `m5.xlarge`, EMR release
  `emr-7.x`, applications Hadoop + Spark. Est. cost ≈ $1.50–2.50/hr of Learner Lab credit.
- Script: `ecommerce_analysis.py` (sent alongside this file) runs all three analyses —
  Event Distribution, Daily Purchase & Revenue, Top 10 Brands by Revenue — against the
  full file. `spark_scalability_benchmark.py` (also sent alongside) re-runs the same
  three analyses at four increasing sample sizes for the scalability numbers in the
  report (Step 4b below, optional but needed for the Table 4.5 / README scalability data).

**Where to run each block:** upload commands run **locally on Windows** (that's where the
file is). Everything else runs in **AWS CloudShell** (already authenticated inside your
Learner Lab console session — no credential setup needed there), matching how Labs 5–6 do it.

---

## Step 0 — Start the Lab + get local AWS CLI credentials (Windows)

1. Go to `awsacademy.instructure.com` → your course → **Learner Lab — Foundational
   Services** → **Start Lab** → wait for the dot to turn green.
2. Click **AWS Details** → next to *AWS CLI*, click **Show** → copy the whole credentials
   block (it has `aws_access_key_id`, `aws_secret_access_key`, `aws_session_token`).
3. On your Windows machine, make sure the AWS CLI is installed:

```powershell
aws --version
# If not found: winget install -e --id Amazon.AWSCLI, then reopen PowerShell
```

4. Create/open the credentials file and paste the block you copied:

```powershell
mkdir $env:USERPROFILE\.aws -Force
notepad $env:USERPROFILE\.aws\credentials
```

Paste, save as:

```ini
[default]
aws_access_key_id = ASIA...
aws_secret_access_key = ...
aws_session_token = ...
```

5. Confirm it works, and note your region (top-right of the AWS Console — commands below
   assume `us-east-1`, change if yours differs):

```powershell
aws sts get-caller-identity
```

> ⚠️ These are **temporary** credentials tied to your Lab session (a few hours). If a
> command suddenly fails with `ExpiredToken` or `InvalidClientTokenId`, go back to AWS
> Details and repaste a fresh block.

---

## Step 1 — Upload 2019-Nov.csv to S3 (Windows, local)

Find the bucket Academy Lab already created for you:

```powershell
aws s3 ls
```

Note its exact name — call it `<BUCKET>` for every command below. Then upload:

```powershell
aws s3 cp "C:\BSDA\Yr3 Sem 8\BIg Data\archive\2019-Nov.csv" s3://<BUCKET>/ecommerce/2019-Nov.csv
```

This is 9GB — expect anywhere from ~10 minutes (fast fibre) to a couple of hours on a
slower upload connection. Let it run to completion; if it drops partway, just rerun the
same command. You can start Step 2 (cluster boot takes ~8 min) while this uploads.

---

## Step 2 — Launch the EMR cluster (AWS CloudShell)

Open the **CloudShell** icon (top-right of the AWS Console). This is a fresh shell, so set
your bucket name again (same one from Step 1):

```bash
BUCKET="<BUCKET>"
```

Find the current EMR release label (don't hardcode a version — it changes over time):

```bash
aws emr list-release-labels --max-items 5 --region us-east-1
```

Pick the newest `emr-7.x.x` value and use it below:

```bash
aws emr create-cluster \
  --name "ist3134-group-assignment" \
  --release-label emr-7.x.x \
  --applications Name=Hadoop Name=Spark \
  --instance-type m5.xlarge \
  --instance-count 3 \
  --service-role EMR_DefaultRole \
  --ec2-attributes KeyName=vockey,InstanceProfile=EMR_EC2_DefaultRole \
  --log-uri s3://$BUCKET/emr-logs/ \
  --region us-east-1
```

`--instance-count 3` with a plain `--instance-type` gives 1 MASTER + 2 CORE automatically
— same shape as Lab 5. This returns a `ClusterId` like `j-XXXXXXXXXXXXX`; save it:

```bash
CLUSTER_ID=j-XXXXXXXXXXXXX

# poll until State is WAITING (~8 min)
aws emr describe-cluster --cluster-id $CLUSTER_ID --query "Cluster.Status.State" --region us-east-1

# once WAITING, grab the primary node's public DNS
aws emr describe-cluster --cluster-id $CLUSTER_ID \
  --query "Cluster.MasterPublicDnsName" --output text --region us-east-1
```

> If `create-cluster` errors with an access-denied on IAM/roles, Academy Lab's restricted
> permissions are blocking the CLI path — fall back to the Console steps from Lab 5 Part 1
> (same settings: name, EMR release, Spark+Hadoop, 1 primary + 2 core m5.xlarge, key pair
> `vockey`, roles `EMR_DefaultRole` / `EMR_EC2_DefaultRole`).

Open SSH on the primary node's security group (Lab 5 Part 2 step 2, as a command):

```bash
SG_ID=$(aws emr describe-cluster --cluster-id $CLUSTER_ID \
  --query "Cluster.Ec2InstanceAttributes.EmrManagedMasterSecurityGroup" --output text --region us-east-1)

aws ec2 authorize-security-group-ingress \
  --group-id $SG_ID --protocol tcp --port 22 --cidr 0.0.0.0/0 --region us-east-1
```

---

## Step 3 — SSH into the primary node (AWS CloudShell)

1. Download `labsuser.pem` from the Learner Lab page (AWS Details → SSH key) and upload it
   into CloudShell via Actions → **Upload file**.
2. Prepare the key and connect:

```bash
mkdir -p ~/.ssh
mv labsuser.pem ~/.ssh/vockey.pem
chmod 400 ~/.ssh/vockey.pem
ssh -i ~/.ssh/vockey.pem hadoop@<PRIMARY_PUBLIC_DNS>
```

---

## Step 4 — Run the analysis (on the EMR primary node, inside the SSH session)

Wait for Step 1's upload to finish before this. Confirm the file landed:

```bash
BUCKET="<BUCKET>"
aws s3 ls s3://$BUCKET/ecommerce/
```

Create the script (paste the whole block — it writes `ecommerce_analysis.py` verbatim):

```bash
mkdir -p ~/workspace/assignment && cd ~/workspace/assignment
cat > ecommerce_analysis.py << 'PYEOF'
# --- paste the full contents of the ecommerce_analysis.py file sent alongside this runbook ---
PYEOF
```

Run it:

```bash
time spark-submit --master yarn --deploy-mode client \
  --num-executors 4 --executor-memory 3g --executor-cores 2 \
  ecommerce_analysis.py \
  s3://$BUCKET/ecommerce/2019-Nov.csv \
  s3://$BUCKET/ecommerce-results
```

This reads the 9GB file, caches it, and runs all three aggregations (Event Distribution,
Daily Purchase & Revenue, Top 10 Brands), printing each result table and timing to the
console as it goes. `--num-executors 4 --executor-memory 3g --executor-cores 2` is a
starting point for 2× m5.xlarge core nodes — if Spark UI (port 18080 / YARN RM UI) shows
stages waiting on resources, scale down; if nodes are idle, scale up.

---

## Step 4b — Run the scalability benchmark (optional, same SSH session)

Same idea as Step 4, but re-runs the same three analyses at four increasing sample sizes
(100K / 500K / 1M / 5M rows) instead of the full file — this is what produces the
scalability table in the report and README. Still on the primary node, same
`~/workspace/assignment` directory:

```bash
cat > spark_scalability_benchmark.py << 'PYEOF'
# --- paste the full contents of the spark_scalability_benchmark.py file sent alongside this runbook ---
PYEOF
```

Run it:

```bash
time spark-submit --master yarn --deploy-mode client \
  --num-executors 4 --executor-memory 3g --executor-cores 2 \
  spark_scalability_benchmark.py \
  s3://$BUCKET/ecommerce/2019-Nov.csv \
  s3://$BUCKET/ecommerce-results/spark-scalability
```

This draws the first 100,000 / 500,000 / 1,000,000 / 5,000,000 rows from the same file
with `.limit()`, materializes each sample into cache before timing starts so the read
itself isn't counted, times all three analyses at each size, and writes one summary CSV
to the output path. Takes a few minutes total — most of it is the same one-time
Spark/YARN startup cost from Step 4, paid once at the start of this run rather than once
per sample size.

---

## Step 5 — Pull results back down and verify

Still on the primary node:

```bash
aws s3 ls s3://$BUCKET/ecommerce-results/ --recursive
```

You should see three folders (`event_distribution`, `daily_purchase_revenue`,
`top10_brands`), each with one `.csv` part file and a `_SUCCESS` marker — plus a fourth
`spark-scalability/` folder if you ran Step 4b.

Back on your **Windows machine** (local PowerShell, not the cluster):

```powershell
mkdir "C:\BSDA\Yr3 Sem 8\BIg Data\results" -Force
aws s3 cp s3://<BUCKET>/ecommerce-results/ "C:\BSDA\Yr3 Sem 8\BIg Data\results\" --recursive
```

---

## Step 6 — Terminate everything — do not skip

EMR keeps billing your Learner Lab credit as long as it runs.

```bash
exit   # leave the SSH session, back in CloudShell

# if this is a new CloudShell tab and $CLUSTER_ID isn't set anymore, look it up:
# aws emr list-clusters --active --region us-east-1 --query "Clusters[*].[Id,Name,Status.State]" --output table

aws emr terminate-clusters --cluster-ids $CLUSTER_ID --region us-east-1

# confirm it's actually going down
aws emr describe-cluster --cluster-id $CLUSTER_ID --query "Cluster.Status.State" --region us-east-1
```

Then on the AWS Academy Learner Lab page, click **End Lab** to stop the billing clock.

---

## Quick troubleshooting notes

- **`ExpiredToken` / `InvalidClientTokenId`** on your local machine → your Learner Lab
  session credentials expired; repaste from AWS Details (Step 0).
- **`aws s3 cp` upload stalls or fails partway** → rerun the same command; it re-uploads
  from scratch (no native resume), so a stable connection matters more than speed.
- **SSH times out** → re-check the security group rule in Step 2 was applied, and that
  you're using the *current* `MasterPublicDnsName` (it can change if the cluster restarted).
- **`spark-submit` runs out of memory / very slow** → lower `--executor-memory` slightly
  (YARN overhead on m5.xlarge leaves less than the full 16GB per node available), or bump
  `--instance-count` to add a third core node before re-running.
- Want to add **2019-Oct.csv** later for the fuller ~14.6GB picture: repeat Step 1 for that
  file, then re-run Step 4 pointing at both paths, e.g.
  `s3://$BUCKET/ecommerce/2019-*.csv` as the input argument (Spark will read both).
