# Threat model

| Threat | Design control | Remaining managed proof |
|---|---|---|
| Training data disclosure | KMS-encrypted S3 and least-privilege roles | IAM/config evidence |
| Online feature enumeration | Entity-scoped access boundary | Request authorization test |
| Future-data leakage | Bitemporal eligibility rule | Managed late-correction test |
| Definition tampering | Immutable definition digest | Artifact provenance evidence |
| Partial generation publication | Exact parity gate and CAS pointer | Failure trace and rollback |
| Stale value serving | TTL and explicit missing result | Measured expiry behavior |

The platform uses synthetic data and makes no claim of regulatory certification.

