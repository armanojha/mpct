#!/usr/bin/env bash
# =============================================================================
# MPCT-AP | entrypoint.sh
#
# WHY dumb-init?
# --------------
# Docker runs PID 1 as your process. The Linux kernel treats PID 1 specially:
# it does NOT automatically reap orphaned child processes. When Playwright
# spawns a Chromium worker and that worker exits abnormally, it becomes a
# zombie (a <defunct> process entry) that holds a PID slot and RSS memory.
#
# Under load with many concurrent scraping sessions, this memory leak can
# trigger an OOM kill of the entire container.
#
# dumb-init sits at PID 1, properly:
#   - Reaps all zombie child processes via waitpid()
#   - Forwards signals (SIGTERM from Cloud Run's graceful shutdown) to the
#     Python subprocess, allowing Uvicorn to drain in-flight requests cleanly.
#
# The `exec` below replaces the shell process with dumb-init, so dumb-init
# itself becomes the direct child of the kernel at PID 1.
# =============================================================================

set -euo pipefail

echo "[entrypoint] Starting MPCT-AP under dumb-init (PID 1 reaper)..."
echo "[entrypoint] Python: $(python --version)"
echo "[entrypoint] Port: ${PORT:-8080}"

exec dumb-init -- uvicorn src.main:app --host 0.0.0.0 --port ${PORT:-8080} --workers 1
