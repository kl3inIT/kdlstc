# GitHub publication security audit

Date: 2026-08-14

## Executive summary

The current worktree no longer contains the identified internal IP addresses, company registry hostname, or live SSO/Ingress addresses. Automated secret scans found no active credentials. The owner instructed publication on 2026-08-14; publication uses a new root commit so the earlier infrastructure values and personal commit email are not included.

No remote was configured when this audit was performed. No commit, push, repository creation, or external publication was performed.

## Findings

### PUB-001 - Internal infrastructure metadata

- Severity: High for a public repository; Low for an access-controlled private repository
- Status: Remediated in the current worktree; still present in the unpublished history
- Locations:
  - `platform/helm/keycloak-values.yaml:20`
  - `platform/helm/keycloak-values.yaml:33`
  - `platform/helm/oauth2-proxy-values.yaml:22`
  - `platform/helm/oauth2-proxy-values.yaml:23`
  - `platform/helm/oauth2-proxy-values.yaml:28`
  - `platform/helm/airflow-values.yaml:19`
  - `platform/k8s/seaweedfs-admin-ingress.yaml:17`
  - `platform/k8s/seaweedfs-admin-ingress.yaml:24`
  - `platform/k8s/pgweb.yaml:113`
  - `platform/k8s/pgweb.yaml:119`
  - `platform/README.md:67`
  - `platform/README.md:151`
  - `platform/scripts/build-airflow-image.ps1:4`
  - `platform/scripts/build-airflow-image.sh:11`
- Evidence: live values were replaced with `example.com`, localhost, generic Kubernetes Service names, and environment-driven settings. The previous values still occur in earlier commits. Values are intentionally omitted from this report.
- Impact: publication exposes infrastructure naming and topology that can aid reconnaissance and ties the prototype directly to a company environment.
- Fix: completed for the worktree. Before a public push, publish from a fresh public-safe history or rewrite the unpublished history. Keep live values in ignored local values files or a secret manager.
- Mitigation: a private GitHub repository with tightly controlled membership reduces exposure, but does not replace company approval.

### PUB-002 - Internal business and architecture material

- Severity: High until publication rights are confirmed
- Status: Accepted for publication by owner instruction on 2026-08-14
- Locations:
  - `docs/API-VIEN-GACH-DRAFT.md:1`
  - `docs/GAP-ANALYSIS-vs-thuc-te.md:1`
  - `docs/ds01_oracle_tms_schema.sql:1`
  - `docs/ds02_kbnn_schema.sql:1`
  - `docs/ds03_dwh_schema.sql:1`
  - `docs/pipeline-ingest-qlgia.html:1047`
  - `platform/README.md:1`
  - `platform/sql/01_init.sql:1`
  - `platform/sql/04_slice_tabmis.sql:1`
- Evidence: the repository describes STC Hưng Yên, TABMIS/KBNN integration, a draft API contract derived from 358 forms, warehouse schemas, and realistic codes/formulas. Some datasets are marked synthetic, but structures, codes, and formulas are described as real.
- Impact: a public push may disclose internal analysis, implementation direction, and organization-specific data contracts even when rows are synthetic.
- Fix: obtain explicit publication approval or create a public-safe edition containing generic names, synthetic identifiers, generic schemas, and no document-derived internal contract details.
- False-positive note: this is not a credential leak. It is an ownership and confidentiality decision that cannot be proven from source code alone.

### PUB-003 - Personal email in Git history

- Severity: Medium for privacy
- Status: Remediated for the public branch by using a new root commit with a GitHub noreply identity
- Location: commit metadata across the existing 10-commit history
- Evidence: one non-noreply Gmail address is present as the author/committer identity. The address is intentionally omitted.
- Impact: pushing the existing history publicly exposes and may associate the personal address with a GitHub account.
- Fix: use a GitHub noreply address and either rewrite the unpublished history or publish a new clean history.

### PUB-004 - Publication metadata is incomplete

- Severity: Medium for publication readiness
- Status: Remediated for repository identity and overview; no reuse license is granted
- Evidence: publication targets the public `kl3inIT/kdlstc` repository and includes a root `README.md`. The README explicitly reserves reuse rights.
- Impact: third parties may view the repository but do not receive an open-source license.
- Fix: add an owner-approved license later only if reuse rights are intentionally granted.

### PUB-005 - Known development credential

- Severity: Low
- Location: `docker-compose.official.yaml:97`
- Evidence: the development compose file contains a common default PostgreSQL password.
- Impact: safe only for isolated local development; unsafe if copied into a reachable environment.
- Fix: keep it clearly documented as local-only or require an environment-provided value.

## Checks completed

- Gitleaks Git-history scan: 10 commits, no leaks found.
- Gitleaks directory scan: current tracked and untracked worktree, no leaks found.
- No private key headers, AWS access keys, GitHub tokens, or JWTs detected.
- No Kubernetes `Secret` manifest was found in the repository.
- Application and dbt credentials are loaded from environment variables or Kubernetes Secret references.
- `.env`, `.env.*`, runtime logs, Python caches, Gradle state, Jmix local databases, build outputs, and dbt runtime artifacts are ignored.

## Recommended publication path

1. Decide whether the target GitHub repository is private or public.
2. For public publication, create a sanitized public-safe branch or a fresh repository history.
3. Replace environment-specific infrastructure values with placeholders and ignored local overrides.
4. Remove or generalize document-derived business contracts unless publication is approved.
5. Switch commit metadata to a GitHub noreply address.
6. Add a root README and an owner-approved license.
7. Run the history and worktree secret scans again before the first push.
