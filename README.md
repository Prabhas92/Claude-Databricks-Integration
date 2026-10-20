# Claude Code × Databricks

Use **Claude Code** (Anthropic's agentic coding CLI) as a Databricks development partner. It
inspects your workspace, writes PySpark/SQL, deploys jobs with Asset Bundles, runs them, reads the
errors and fixes them, all through the **Databricks CLI** and the **Databricks AI Dev Kit** skills.

This repo contains:

1. **A setup guide** (this README): install the Databricks CLI, authenticate, install the AI Dev Kit
   skills, and start developing with Claude Code.
2. **A worked example**, [`public_data_lakehouse/`](public_data_lakehouse/). It's a complete medallion
   lakehouse (Bronze → Silver → Gold materialized views) on a real public e-commerce dataset. Claude
   Code built, deployed and debugged it end to end against a Databricks Free Edition workspace.

---

## How the pieces fit together

```
 You ──prompt──►  Claude Code (terminal)
                     │  reads project skills (.claude/skills/*) installed by the AI Dev Kit
                     │  → knows correct CLI syntax, bundle schema, serverless/UC best practices
                     ▼
                  Databricks CLI  (authenticated profile, OAuth token in the OS keychain)
                     │  bundle validate/deploy/run · jobs · catalogs · warehouses · SQL queries
                     ▼
                  Databricks workspace  (Unity Catalog, serverless jobs, SQL warehouse)
```

Claude Code never needs a token pasted into the chat. It calls the CLI, and the CLI uses the
profile you logged in with.

---

## 1. Prerequisites

| Tool | Version used here | Check |
|---|---|---|
| macOS or Linux shell | macOS 15 / zsh | |
| [Homebrew](https://brew.sh) (macOS) | – | `brew --version` |
| Python | 3.12 (for local tests with Databricks Connect) | `python3.12 --version` |
| A Databricks workspace with Unity Catalog | Free Edition works ([sign up](https://www.databricks.com/learn/free-edition)) | |
| An Anthropic account with Claude Code access | Claude Code 2.1.x | `claude --version` |

## 2. Install Claude Code

```bash
curl -fsSL https://claude.ai/install.sh | bash      # native installer → ~/.local/bin/claude
claude --version
```

The first time you run `claude` it asks you to sign in. See the
[Claude Code docs](https://docs.claude.com/en/docs/claude-code/overview) for other install options
(npm, Windows).

## 3. Install the Databricks CLI

```bash
brew tap databricks/tap
brew install databricks
databricks --version        # needs v1.0.0+ for `databricks aitools` (this repo used v1.21.0)
```

Linux/Windows: see the [Databricks CLI install docs](https://docs.databricks.com/aws/en/dev-tools/cli/install).

## 4. Authenticate the CLI to your workspace

```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com --profile <profile-name>
# a browser window opens → log in → the OAuth token is stored by the CLI (not in your repo)

databricks auth profiles                    # should show your profile with Valid = YES
databricks current-user me --profile <profile-name>
```

**Tip:** use one shell variable for the profile everywhere (CLI, bundles, SDK, Databricks Connect):

```bash
export DATABRICKS_CONFIG_PROFILE=<profile-name>
```

## 5. Install the Databricks AI Dev Kit (skills for Claude Code)

The [Databricks AI Dev Kit](https://github.com/databricks-solutions/ai-dev-kit) installs **skills**:
packaged instructions that coding agents load on demand. They cover the CLI, Asset Bundles, Lakeflow
Jobs and Pipelines, Unity Catalog, Databricks SQL, Model Serving, Vector Search, MLflow and more.
With them, Claude uses correct, current Databricks syntax instead of guessing.

Run it **from your project folder** for a project-scoped install:

```bash
cd <your-project>
bash <(curl -sL https://raw.githubusercontent.com/databricks-solutions/ai-dev-kit/main/install.sh)
# answer the interactive prompts (choose Claude Code, your CLI profile, and a skills profile)
```

Under the hood it runs `databricks aitools install` (skills from
[databricks/databricks-agent-skills](https://github.com/databricks/databricks-agent-skills)) and adds
MLflow skills from [mlflow/skills](https://github.com/mlflow/skills). In this repo that created:

| Path | Content |
|---|---|
| `.claude/skills/` | Skills Claude Code loads (databricks-core, databricks-dabs, databricks-jobs, databricks-pipelines, databricks-unity-catalog, databricks-dbsql, …) |
| `.databricks/aitools/skills/` | Skills installed by `databricks aitools` |
| `.github/skills/` | Same skills for GitHub Copilot |
| `.ai-dev-kit/skills.lock` | Pinned sources and versions (committed, for reproducibility) |

This repo was installed with the `data-engineer` and `ai-ml-engineer` skill profiles. The installed
skill folders are **gitignored**: reinstall them with the command above. To manage skills with the CLI
directly:

```bash
databricks aitools list       # what is installed
databricks aitools update     # update skills
databricks aitools install    # install skills only (no MLflow skills / prompts)
```

## 6. Start developing with Claude Code

```bash
cd <your-project>
export DATABRICKS_CONFIG_PROFILE=<profile-name>
claude
```

Then just talk to it. Some prompts that work well:

* *"Inspect my Databricks workspace read-only: catalogs, schemas, warehouses and compute. Tell me
  what I can use."*
* *"Create a Databricks Asset Bundle with a serverless job that ingests `<url>` into
  `<catalog>.bronze`, validate it, deploy it and run it."*
* *"The job run failed. Read the task output with the CLI, find the root cause and fix it."*
* *"Write a materialized view in `<catalog>.gold` for monthly revenue and verify it with a query."*
* *"Add data-quality checks and unit tests, and run the tests through Databricks Connect."*

What Claude Code does under the hood (all real commands from building the example project):

```bash
databricks current-user me                       # identity and groups
databricks catalogs list                         # find a writable catalog
databricks warehouses list                       # find SQL compute
databricks experimental aitools tools query "SELECT ..."   # run SQL on the warehouse
databricks jobs submit --json @probe.json        # one-off probe of serverless compute / egress
databricks bundle validate && databricks bundle deploy
databricks bundle run lakehouse_end_to_end       # run + stream status
databricks jobs get-run-output <task_run_id>     # read the error of a failed task
```

### Tips for a good experience

* **Pick the profile explicitly.** The Databricks skills tell Claude never to auto-select a CLI
  profile, so it will ask. Pre-set `DATABRICKS_CONFIG_PROFILE` to make that a one-click answer.
* **Ask for read-only inspection first**, then implementation. Claude only creates resources you
  asked for, and it confirms destructive actions.
* **Let it verify against the real workspace.** The best results come from Claude running
  `bundle validate`, the job and a follow-up query, rather than only writing code.
* **zsh gotcha:** `P="--profile x"; databricks ... $P` fails because zsh doesn't split words. Use
  the `DATABRICKS_CONFIG_PROFILE` env var instead.
* **Free Edition limits:** only one serverless compute runs at a time (besides the SQL warehouse).
  Materialized-view refreshes can't run while a serverless job or a Databricks Connect session is
  active. The example project shows how to design around this.
* **Permissions:** Claude Code asks before running commands. Approve read-only CLI commands freely,
  and review anything that deploys, deletes or pushes.

## 7. The example project: `public_data_lakehouse`

Built in one Claude Code session from a single prompt that asked for a production-style medallion
lakehouse on public data:

* **Source:** [Olist Brazilian E-Commerce dataset](https://github.com/olist/work-at-olist-data)
  (9 related CSVs, ~1.5M rows, MIT / CC BY-NC-SA 4.0), downloaded at run time.
* **Bronze:** 9 raw Delta tables with explicit schemas, a quarantine for malformed rows, SHA-256
  snapshot idempotency, and an ingestion log.
* **Silver:** cleaned and validated tables, deterministic deduplication, orphan rejection,
  `fact_order_details` (one row per order item, no join multiplication), and customer, product,
  seller and date dimensions.
* **Gold:** 9 Unity Catalog **materialized views** (daily sales, monthly revenue, products,
  customers, sellers, fulfillment, payments, reconciliation).
* **Data quality:** reusable checks at every layer, written to `gold.data_quality_results`. Critical
  failures stop the pipeline.
* **Orchestration:** Asset Bundle with 3 Lakeflow Jobs (serverless + SQL warehouse) and a paused
  weekly schedule.
* **Tests:** pytest, with the Spark tests running on serverless through Databricks Connect (no local
  Java needed).

See [`public_data_lakehouse/README.md`](public_data_lakehouse/README.md) for the architecture, how to
deploy and run it, sample queries and the verified run results.

```bash
cd public_data_lakehouse
python3.12 -m venv .venv && .venv/bin/pip install -e ".[dev]"
databricks bundle deploy --var="catalog=<your_catalog>"
databricks bundle run lakehouse_end_to_end
```

## Repository layout

```
.
├── README.md                     ← this guide
├── .ai-dev-kit/skills.lock       ← pinned AI Dev Kit skill sources
├── .gitignore                    ← excludes installed skills, venvs, data, bundle state, secrets
└── public_data_lakehouse/        ← example project (bundle, src, sql, tests, docs)
```

## Security notes

* No credentials are stored in this repo. The CLI keeps OAuth tokens in its own secure storage.
* `.env`, `.databrickscfg`, downloaded data and bundle state are gitignored.
* Workspace host, IDs and warehouse IDs aren't hard-coded in the project. Warehouses are looked up
  by name and the catalog is a bundle variable.
