# Native three-video review

`send_video_choices_cli.py` runs once in the existing DEV service runtime. It sends
three distinct MP4 attachments in one Telegram Bot API rich message, with native
`Выбрать A/B/C` buttons. `q:<group uid>:1..3` uses the existing callback handler:
the sender receives `kind=pick`, text `1..3`, scoped to its own agent and group.
Map those values to the returned ordered variant descriptors and original SHA256.

This operation does not restart the DEV service, register another webhook,
change bot permissions, or restart render jobs. It does not expose a persistent
new HTTP endpoint: the temporary FastAPI route is called inside the one-shot
process only. Keep request files and test state under the service/project folder.

Use the service's configured user and existing DEV environment via systemd;
never copy or print its token. Only the DEV environment is needed, not model
download or publishing-provider credentials. A sender exception is sanitized.
For an unknown delivery outcome, inspect the exact group metadata and the own
scoped history before retrying. A saved `sent` receipt is recovered idempotently.

Tests run in the production Python runtime with mocked Telegram calls, isolated
SQLite, and the actual existing callback's AST (without importing its private
configuration). They check native media, buttons, sender scope and selected-file
identity. Run `python -B test_video_variants.py` in the isolated staging folder.

The approved three candidate files remain separate; do not replace them with
one stitched comparison. Do not press your own selection/acceptance buttons.
