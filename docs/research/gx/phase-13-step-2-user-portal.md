# Phase 13 - Step 2: GX on the User Portal

The upload history API now exposes the GX result produced by
`gx_validate_file_upload` for the exact `universal_lakehouse_pipeline` run.
The User Portal displays that result as the fifth pipeline stage and as a
quality-gate summary below the pipeline timeline.

## Status semantics

- `PASSED`: GX validated the dataset and the response explicitly allows the
  downstream publish/merge action.
- `FAILED` or `BLOCKED`: validation failed, or the GX task could not run because
  an upstream task failed.
- `NOT_CONFIGURED`: no GX contract is registered for this dataset. This is shown
  as a warning and must not be interpreted as a GX pass.
- `RUNNING` or `PENDING`: validation has not reached a terminal result.
- `RESULT_UNAVAILABLE`: Airflow reports task success but its GX XCom result is
  unavailable. The portal deliberately does not infer a pass.
- `NOT_AVAILABLE`: the selected historical DAG run has no GX task.

## API boundary

The backend reads only the return XCom of `gx_validate_file_upload`. It exposes a
bounded allowlist of status, dataset identity, validation identity, result URI,
and publish decision fields. Raw rows and arbitrary XCom fields are not returned
to the browser.

## Acceptance check

1. Upload a file that maps to a registered GX dataset.
2. Open that upload in **Lịch sử tải lên**.
3. Confirm the pipeline includes **GX Quality**.
4. Confirm the GX panel shows the dataset, validation ID, and `ALLOWED` or
   `BLOCKED` decision.
5. For an unregistered dataset, confirm the portal shows `NOT_CONFIGURED` and the
   warning that the result is not a GX pass.
