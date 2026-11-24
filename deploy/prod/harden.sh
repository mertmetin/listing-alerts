#!/usr/bin/env bash
# Hardens a fresh AlmaLinux 9 VPS for listing-alerts. Idempotent: safe to
# re-run. Run as root on the server.
#
#   - SSH: keys only (no passwords), root with keys only
#   - firewalld: only SSH, HTTP, HTTPS
#   - fail2ban on SSH
#   - automatic security updates (dnf-automatic)
#   - 2 GB swap file
#   - Docker enabled at boot
set -euo pipefail

echo "== SSH: keys only"
# sshd uses the first value it reads, and drop-ins are read before the main
# file's own "PermitRootLogin yes", so a 01- file wins.
cat >/etc/ssh/sshd_config.d/01-hardening.conf <<'EOF'
PasswordAuthentication no
KbdInteractiveAuthentication no
PermitRootLogin prohibit-password
X11Forwarding no
MaxAuthTries 4
EOF
chmod 600 /etc/ssh/sshd_config.d/01-hardening.conf
sshd -t
systemctl reload sshd
sshd -T | grep -E '^(passwordauthentication|permitrootlogin|kbdinteractiveauthentication) '

echo "== firewall: ssh, http, https"
dnf -q -y install firewalld >/dev/null
# Allow SSH in the saved config before the firewall starts, so enabling it
# can never cut off the session running this script.
if ! systemctl is-active -q firewalld; then
  firewall-offline-cmd -q --add-service=ssh --add-service=http --add-service=https
fi
systemctl enable --now firewalld >/dev/null
firewall-cmd -q --permanent --add-service=ssh --add-service=http --add-service=https
# AlmaLinux opens the Cockpit web console (port 9090) by default; not used here.
firewall-cmd -q --permanent --remove-service=cockpit 2>/dev/null || true
firewall-cmd -q --reload
firewall-cmd --list-services

echo "== fail2ban on sshd"
dnf -q -y install fail2ban fail2ban-firewalld >/dev/null
cat >/etc/fail2ban/jail.d/sshd.local <<'EOF'
[sshd]
enabled = true
maxretry = 5
findtime = 10m
bantime = 1h
EOF
systemctl enable --now fail2ban >/dev/null
systemctl restart fail2ban
for _ in 1 2 3 4 5 6 7 8 9 10; do fail2ban-client ping >/dev/null 2>&1 && break; sleep 1; done
fail2ban-client status sshd | grep -E 'Currently|Total' || true

echo "== automatic security updates"
dnf -q -y install dnf-automatic >/dev/null
sed -i -e 's/^upgrade_type.*/upgrade_type = security/' -e 's/^apply_updates.*/apply_updates = yes/' /etc/dnf/automatic.conf
systemctl enable --now dnf-automatic.timer >/dev/null
systemctl is-active dnf-automatic.timer

echo "== swap"
if ! swapon --show | grep -q /swapfile; then
  fallocate -l 2G /swapfile
  chmod 600 /swapfile
  mkswap /swapfile >/dev/null
  swapon /swapfile
  grep -q '^/swapfile' /etc/fstab || echo '/swapfile none swap sw 0 0' >>/etc/fstab
fi
sysctl -q -w vm.swappiness=10
echo 'vm.swappiness=10' >/etc/sysctl.d/90-swappiness.conf
swapon --show

echo "== docker at boot"
systemctl enable --now docker >/dev/null
systemctl is-active docker
echo "done"
