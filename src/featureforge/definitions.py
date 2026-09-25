"""Versioned feature definitions for the payment-risk example domain."""

from featureforge.model import FeatureDefinition

DAY = 86_400


def payment_features() -> tuple[FeatureDefinition, ...]:
    return (
        FeatureDefinition(
            "transaction_count_24h",
            1,
            "integer",
            2 * DAY,
            "transaction_count",
            DAY,
            "Transactions observed during the prior 24 hours.",
            0,
        ),
        FeatureDefinition(
            "successful_spend_30d_cents",
            1,
            "integer",
            31 * DAY,
            "successful_spend_cents",
            30 * DAY,
            "Successful payment amount during the prior 30 days.",
            0,
        ),
        FeatureDefinition(
            "failed_payment_ratio_7d",
            1,
            "float",
            8 * DAY,
            "failed_payment_ratio",
            7 * DAY,
            "Share of observed payments that failed during the prior seven days.",
            0.0,
        ),
        FeatureDefinition(
            "hours_since_successful_payment",
            1,
            "float",
            31 * DAY,
            "hours_since_success",
            30 * DAY,
            "Hours since the most recent successful payment.",
            None,
        ),
        FeatureDefinition(
            "max_merchant_risk_7d",
            1,
            "float",
            8 * DAY,
            "max_merchant_risk",
            7 * DAY,
            "Maximum merchant risk observed during the prior seven days.",
            None,
        ),
    )
