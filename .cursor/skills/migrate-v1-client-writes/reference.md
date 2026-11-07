# Extra write notes

Worked examples and the mutation-to-`InsertSettings` map are in [SKILL.md](SKILL.md). Use those.

`DataStreamWriter.onOutput` runs when the HTTP body is written. The client closes the stream. With client compression, that stream is already compressing unless the operation sets app-compressed data.

A callback that builds rows can run again on retry. The `InputStream` insert calls `reset()` from `onRetry`, so a stream that cannot reset fails on retry.
