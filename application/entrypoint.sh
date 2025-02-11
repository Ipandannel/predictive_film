#!/bin/sh
python import_data.py &  # Run in background
exec python app.py        # Start Flask immediately
