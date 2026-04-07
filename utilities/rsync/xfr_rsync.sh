#!/bin/bash

# Bash script to xfr chemview archive to 
# a remote torrent server.
# This is Anne Gunn's adaptation of what Gemini referred to
# as a 'gold standard' version. It will keep retrying rsync 
# as needed until rsync's exit status is 0 (success).
# I did have to restart the tmux/script combo a few times but 
# the transfer progressed smoothly across 10+ days and at least
# 2 Windows reboots due to updates.

# I ran this in my WSL Ubuntu. It's not a Windows script.
# You'll need WSL or a native Linux-ish OS to use it. 

# Be sure the ssh agent is running before the loop
# starts so you don't have to enter your passphrase
# each time the rsync has to restart.
# On your local machine you would do something like:
# eval "$(ssh-agent -s)"
# ssh-add ~/.ssh/id_ed25519
# You'll be prompted once for the passphrase

# Also, run this within a tmux session so the terminal
# window doesn't have to stay open for the whole multiday session.
# tmux new -s chemview_xfr

# Run this from within the chemview archive
# folder you wish to transfer. For me this was 
# /mnt/h/openSource/dataPreservation/chemView/harvest/chemview_merged

# Configuration
# Make sure the DEST folder already exists on the remote server
SOURCE="/mnt/h/openSource/dataPreservation/chemView/harvest/chemview_merged/"
DEST="<your remote id>:~/storage/chemview_20251231"
LOG="/mnt/c/openSource/dataPreservation/chemView/harvest/rsync_upload.log"
PORT=8122

# Speed Settings (in KiB/s) to ensure rsync doesn't swamp 
# the local (presumably home) internet connection during the day.
# Note that the script only pays attention to night/day times
# at execution. So, in practice, if you want to see the limit
# changed, attach to your tmux session (above)
#    tmux attach chemview_xfr
# then press ctrl-c ONCE.
# Script should stop, then restart (within a minute?) with
# a new, time-of-day appropriate limit.
DAY_LIMIT=650    # ~5 Mbps (Very safe for daytime)
NIGHT_LIMIT=1200 # ~9.6 Mbps (Fast for overnight)

echo "Starting transfer management script at $(date)" | tee -a "$LOG"

while true; do
    # Get current hour in 24h format (00-23)
    CURRENT_HOUR=$(date +%H)
    
    # Determine which limit to use
    if [ "$CURRENT_HOUR" -ge 8 ] && [ "$CURRENT_HOUR" -lt 22 ]; then
        CURRENT_LIMIT=$DAY_LIMIT
        echo "Day mode: Limiting to ${CURRENT_LIMIT}KiB/s"
    else
        CURRENT_LIMIT=$NIGHT_LIMIT
        echo "Night mode: Increasing to ${CURRENT_LIMIT}KiB/s"
    fi

    # Run rsync. 
    # Note: If it finishes successfully, we break the loop.
    # If it fails (connection drop or time-out), the loop restarts it with the correct limit for the new hour.
	# -avP is archive + verbose + partial + progress
    rsync -avP \
        --bwlimit=$CURRENT_LIMIT \
        --log-file="$LOG" \
        -e "ssh -p $PORT -o ServerAliveInterval=60" \
        "$SOURCE" "$DEST"

    # Check the exit code of rsync
    if [ $? -eq 0 ]; then
        echo "Transfer complete at $(date)!" | tee -a "$LOG"
        break
    else
        echo "Transfer paused/interrupted at $(date). Retrying in 60 seconds..." | tee -a "$LOG"
        sleep 60
    fi
done