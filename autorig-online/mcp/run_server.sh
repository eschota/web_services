#!/bin/bash
cd /root/autorig-online/mcp
export PYTHONPATH=/root/autorig-online/mcp/src:$PYTHONPATH
exec /root/autorig-online/mcp/.venv/bin/python3 -c "from renderfin_mcp.server import main; main()"
