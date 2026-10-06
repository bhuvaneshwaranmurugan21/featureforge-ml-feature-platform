# Stage 6 autonomous read-only evidence recovery

The Stage 6 qualification workflow includes an independent recovery job on the
existing qualification branch. The job uses the existing repository-bound OIDC
role, repository-pinned dependencies, and a 15-minute execution limit. It does
not require CloudShell commands or permanent AWS credentials.

The collector fixes its account, region, bucket and two object keys in code. It
admits only the existing FeatureForge OIDC role, validates immutable version
inventories before and after retrieval, and reads specific object versions. The
state must retain exactly its one authorized bootstrap version. The lease may
retain later immutable successor versions; the collector locates the original
historical lease by its pinned version fingerprint and proves that version is
still present without treating it as current authority.
No AWS mutation, Terraform invocation, lease renewal, managed workload or
other-project operation is implemented. Only a sanitized receipt is emitted to
the job log; raw lease IDs, state bytes, account IDs and ARNs are not published.

Historical plan source commit `60c9fb508548470943ba6c66cec6774cc86ad3c0` and tree
`66ab91cefa34fd74403750f4d63d193861a5b289` remain unchanged. The recovery executor
commit is recorded separately. An expired lease supports historical verification
only; it never grants current plan, apply, or deployment authority.

Successful collection is not Stage 6 completion. Exact original plan and receipt
digests, predecessor gates, protected exact-head CI/review, merged-main
verification and the continuation receipt remain necessary. Missing or
ambiguous historical proof is a failure, not permission to recreate a lease or
rerun the plan.

Routine FeatureForge branch work, validation, evidence retrieval and compliant
PR merging may proceed within the user's standing delegation. AWS writes,
deployment, IAM changes and security-gate changes are not authorized by this
read-only execution path.
