# Thoughtfull

Thoughtfull is a distributed job control orchestrator.

It is designed to manage images, sandboxes, and processes running locally or in the cloud.

## Features

- Create and manage images, sandboxes, and both local and remote processes
- Snapshot existing sandboxes and restore them later
- Easily connect to any running sandbox or process
- Runs processes in tmux in order to collect output from interactive processes (while enabling stdin)
- Supports fully hierarchical trees of sandboxes and processes with "structured concurrency" semantics
- Continually syncs logs and output from remote processes
- Support modal, fly.io, docker, and "local" sandboxes

# Architecture

- Uses tmux to run processes remotely in a way where they can be attached to later
- Uses rsync to sync files and directories between local and remote machines
- Uses SSH to connect to remote machines
