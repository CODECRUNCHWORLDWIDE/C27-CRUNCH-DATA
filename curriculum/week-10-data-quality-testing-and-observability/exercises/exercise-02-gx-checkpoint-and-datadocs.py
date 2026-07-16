"""
Exercise 02 — Wire a GX Checkpoint, run validations, and render Data Docs.

GOAL
    Take the "orders_ingestion" suite from Exercise 01 and make it a runnable,
    reportable GATE: a Checkpoint that runs the validation, fires an Action to
    rebuild Data Docs on every run, and returns a CheckpointResult whose
    `.success` your pipeline can `raise` on. Then open the Data Docs site — the
    human-readable DQ report from Lecture 1 section 6.

PREREQUISITES
    Exercise 01 complete (the ./gx/ context and "orders_ingestion" suite exist).
    pip install "great_expectations>=1.0" pandas

RUN
    python exercise-02-gx-checkpoint-and-datadocs.py
    # Runs the checkpoint on a clean batch (PASS) and a corrupted batch (FAIL),
    # rebuilds Data Docs, and prints the path to the HTML report.

WHAT TO COMPLETE
    Every `# TODO`. The checkpoint must:
      - run the orders_ingestion validation,
      - include an UpdateDataDocsAction,
      - use result_format SUMMARY (so failures include sample bad values),
      - and the script must turn a failed result into a raised exception (the GATE).

DELIVERABLE
    A Checkpoint "orders_ingestion_checkpoint" persisted under ./gx/, a Data
    Docs site you can open, and a screenshot/paste of the failing validation
    page showing which expectations failed and the offending sample values.
"""

import great_expectations as gx
from great_expectations.checkpoint import UpdateDataDocsAction

# Reuse the helpers from exercise 01 so the two exercises stay consistent.
from exercise_01_ingestion_gx_suite import (  # noqa: F401  (rename file or copy if import fails)
    build_context_and_batch,
    build_suite,
    clean_orders,
    corrupted_orders,
)


def build_validation_definition(context, batch_definition, suite):
    """Bind the suite to the batch (fetch-or-add)."""
    name = "orders_ingestion_validation"
    try:
        return context.validation_definitions.get(name)
    except Exception:
        # TODO: add and return a gx.ValidationDefinition(name=name,
        #        data=batch_definition, suite=suite)
        raise NotImplementedError("complete build_validation_definition()")


def build_checkpoint(context, validation_definition):
    """A Checkpoint that runs the validation + rebuilds Data Docs, with SUMMARY results."""
    name = "orders_ingestion_checkpoint"
    try:
        return context.checkpoints.get(name)
    except Exception:
        pass
    # TODO: add and return a gx.Checkpoint with:
    #   name=name,
    #   validation_definitions=[validation_definition],
    #   actions=[UpdateDataDocsAction(name="refresh_docs")],
    #   result_format={"result_format": "SUMMARY"},
    raise NotImplementedError("complete build_checkpoint()")


def gate(result) -> None:
    """The halting gate (Lecture 1 section 5.1): raise on a failed checkpoint."""
    if result.success:
        return
    # TODO: collect the failed expectation types from result.run_results and
    #       raise RuntimeError(f"orders_ingestion checkpoint FAILED: {failed}")
    #   failed = [r.expectation_config.type
    #             for run in result.run_results.values()
    #             for r in run.results if not r.success]
    raise NotImplementedError("complete gate()")


if __name__ == "__main__":
    context, batch_definition = build_context_and_batch()
    suite = build_suite(context)
    validation_definition = build_validation_definition(context, batch_definition, suite)
    checkpoint = build_checkpoint(context, validation_definition)

    print("=== CLEAN batch ===")
    clean = checkpoint.run(batch_parameters={"dataframe": clean_orders()})
    print("success:", clean.success)

    print("\n=== CORRUPTED batch ===")
    bad = checkpoint.run(batch_parameters={"dataframe": corrupted_orders()})
    print("success:", bad.success)

    # Rebuild + locate the Data Docs report.
    context.build_data_docs()
    print("\nData Docs at: gx/uncommitted/data_docs/local_site/index.html")
    # context.open_data_docs()  # uncomment to open in a browser locally

    # Prove the gate halts on bad data.
    gate(bad)
