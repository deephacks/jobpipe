# jobpipe
Java scheduler for pipelines of long-running batch processes, inspired by Spotify [Luigi](https://github.com/spotify/luigi).

The purpose of jobpipe is to execute certain tasks at regular time intervals and allow expressing dependencies
between tasks as sequence of continuous executions in time. Every task produce output which is provided as input to dependent tasks. Any task is stalled until dependent task output is produced (if any). Idempotent task output enable resumability of pipelines that crash halfway. Task that fail (without a previously valid output) will transitively fail its dependent tasks.


