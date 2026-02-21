#!/bin/bash

# Configuration
PROJ_DIR="/root/trading/db_utils"
PYTHON_BIN="$PROJ_DIR/venv/bin/python"
USER="root"

create_service() {
    local name=$1
    local script=$2
    cat <<EOF > /etc/systemd/system/$name.service
[Unit]
Description=SidBot Service: $script
After=network.target

[Service]
Type=oneshot
User=$USER
WorkingDirectory=$PROJ_DIR
ExecStart=$PYTHON_BIN $script
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF
}

create_timer() {
    local name=$1
    local calendar=$2
    cat <<EOF > /etc/systemd/system/$name.timer
[Unit]
Description=Timer for $name

[Timer]
OnCalendar=$calendar
Persistent=true
Unit=$name.service

[Install]
WantedBy=timers.target
EOF
}

# 1. Scanner: Runs at 9am, 12pm, 3pm, and 6pm M-F
create_service "sidbot-scanner" "sidbot_scanner.py"
create_timer "sidbot-scanner" "Mon..Fri 09,12,15,18:00:00"

# 2. Reporter: Runs at 15:30 M-F
create_service "sidbot-reporter" "sidbot_reporter.py"
create_timer "sidbot-reporter" "Mon..Fri 15:30:00"

# 3. Daily DB Update: Runs at 16:30 M-F (Post-close)
create_service "sidbot-daily-update" "daily_db_update.py"
create_timer "sidbot-daily-update" "Mon..Fri 16:30:00"

# 4. Weekly Maintenance: Runs Sundays at 4am
create_service "sidbot-maintenance" "maintain_db.py"
create_timer "sidbot-maintenance" "Sun 04:00:00"

# 5. Earnings Fetch: Chained to run after maintenance
create_service "sidbot-earnings" "fetch_earnings.py"
# We don't need a separate timer for earnings if we chain it:
sed -i "/\[Service\]/a ExecStartPost=/usr/bin/systemctl start sidbot-earnings.service" /etc/systemd/system/sidbot-maintenance.service

# Reload and Start
systemctl daemon-reload
systemctl enable --now sidbot-scanner.timer sidbot-reporter.timer sidbot-daily-update.timer sidbot-maintenance.timer

echo "✅ SidBot Automation Suite deployed."
systemctl list-timers "sidbot*"