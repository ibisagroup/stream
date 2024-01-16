#!/bin/bash
exec python3 data.py &
exec python3 api.py &
wait