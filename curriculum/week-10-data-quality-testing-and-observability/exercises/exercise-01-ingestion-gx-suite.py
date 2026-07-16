"""
Exercise 01 — Build a Great Expectations suite over the raw orders ingestion.

GOAL
    Author a GX Core 1.x Expectation Suite that gates the raw `orders` data
    BEFORE it enters staging. One expectation per data-quality dimension from
    Lecture 1: completeness, validity, uniqueness, volume. This is the
    ingestion-boundary gate.

PREREQUISITES
    pip install "great_expectations>=1.0" pandas
    (GX 1.x ONLY — 0.x snippets will NOT run here. See Lecture 2 section 1.)

RUN
    python exercise-01-ingestion-gx-suite.py
    # On first run this creates ./gx/ (the file-backed Data Context).
    # It validates a clean sample (should PASS) and a corrupted sample
    # (should FAIL) so you can see the gate fire.

WHAT TO COMPLETE
    Every block marked `# TODO`. The clean sample should pass all
    expectations; the corrupted sample should fail at least three of them
    (a null order_id, a bad status, a negative total, a duplicate, and a
    short row count).

DELIVERABLE
    A passing suite named "orders_ingestion" persisted under ./gx/, and a
    short note (in SOLUTIONS or your repo) listing which expectation each
    corruption tripped.
"""

import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd


def clean_orders() -> pd.DataFrame:
    """A well-formed nightly load: ~40k rows is normal; we use a small clean sample."""
    return pd.DataFrame(
        {
            "order_id":     [1001, 1002, 1003, 1004],
            "line_number":  [1, 1, 1, 2],
            "customer_id":  [42, 7, 42, 42],
            "status":       ["PLACED", "SHIPPED", "DELIVERED", "DELIVERED"],
            "total_cents":  [1999, 500, 8200, 1500],
            "currency_code": ["USD", "USD", "EUR", "EUR"],
        }
    )


def corrupted_orders() -> pd.DataFrame:
    """A malformed load: a null key, a typo status, a negative total, a dup key, a bad currency."""
    return pd.DataFrame(
        {
            "order_id":     [1001, None, 1003, 1003],          # null + duplicate (1003,1)
            "line_number":  [1, 1, 1, 1],
            "customer_id":  [42, 7, 42, 42],
            "status":       ["PLACED", "PLCAED", "DELIVERED", "DELIVERED"],  # typo
            "total_cents":  [1999, 500, -8200, 1500],          # negative
            "currency_code": ["USD", "usd", "EUR", "EUR"],     # lowercase fails ^[A-Z]{3}$
        }
    )


def build_context_and_batch():
    """Create a file-backed Data Context, a pandas Data Source, asset, and batch definition."""
    context = gx.get_context(mode="file")

    # TODO: add a pandas data source named "raw_orders_source".
    #   data_source = context.data_sources.add_pandas(name=...)
    # If it already exists on a re-run, fetch it instead:
    #   data_source = context.data_sources.get("raw_orders_source")
    data_source = ...

    # TODO: add a dataframe asset named "raw_orders" on that source.
    data_asset = ...

    # TODO: add a whole-dataframe batch definition named "nightly_batch".
    batch_definition = ...

    return context, batch_definition


def build_suite(context) -> gx.ExpectationSuite:
    """One expectation per dimension. Fetch-or-create so re-runs don't error."""
    try:
        return context.suites.get("orders_ingestion")
    except Exception:
        pass

    suite = context.suites.add(gx.ExpectationSuite(name="orders_ingestion"))

    # --- COMPLETENESS ---
    # TODO: order_id must never be null.
    #   suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="order_id"))
    # TODO: customer_id may be up to 1% null (use mostly=0.99).

    # --- VALIDITY ---
    # TODO: status must be in {PLACED, SHIPPED, DELIVERED, CANCELLED}
    #       (gxe.ExpectColumnValuesToBeInSet).
    # TODO: total_cents must be between 0 and 10_000_000
    #       (gxe.ExpectColumnValuesToBeBetween).
    # TODO: currency_code must match ^[A-Z]{3}$
    #       (gxe.ExpectColumnValuesToMatchRegex).

    # --- UNIQUENESS ---
    # TODO: (order_id, line_number) must be unique
    #       (gxe.ExpectCompoundColumnsToBeUnique).

    # --- VOLUME ---
    # NOTE: with the tiny sample above, use a small band so clean passes.
    # TODO: row count between 1 and 100 (gxe.ExpectTableRowCountToBeBetween).
    #       In production this band is your contract's 30_000–50_000.

    suite.save()
    return suite


def validate(context, batch_definition, suite, df: pd.DataFrame):
    """Bind suite to batch via a Validation Definition, run it, return the result."""
    # TODO: create a ValidationDefinition (fetch-or-add) binding `suite` to
    #       `batch_definition`, then run it with batch_parameters={"dataframe": df}.
    #   validation_definition = context.validation_definitions.add(
    #       gx.ValidationDefinition(name="orders_ingestion_validation",
    #                               data=batch_definition, suite=suite))
    #   return validation_definition.run(batch_parameters={"dataframe": df})
    raise NotImplementedError("complete validate()")


if __name__ == "__main__":
    context, batch_definition = build_context_and_batch()
    suite = build_suite(context)

    print("=== CLEAN load (expect success=True) ===")
    clean_result = validate(context, batch_definition, suite, clean_orders())
    print("success:", clean_result.success)

    print("\n=== CORRUPTED load (expect success=False) ===")
    bad_result = validate(context, batch_definition, suite, corrupted_orders())
    print("success:", bad_result.success)

    # The GATE (Lecture 1 section 5): in a pipeline you would raise here.
    if not bad_result.success:
        print("GATE WOULD HALT THE PIPELINE — the corrupted load is rejected.")
