#!/usr/bin/env bash
#
# Initialize Superset database and admin user
#
set -e

# Install local requirements first
REQUIREMENTS_LOCAL="/app/docker/requirements-local.txt"
if [ -f "${REQUIREMENTS_LOCAL}" ]; then
  echo "Installing local overrides at ${REQUIREMENTS_LOCAL}"
  if command -v uv > /dev/null 2>&1; then
    uv pip install --no-cache-dir -r "${REQUIREMENTS_LOCAL}"
  else
    pip install --no-cache-dir -r "${REQUIREMENTS_LOCAL}"
  fi
fi

STEP_CNT=3

echo_step() {
cat <<EOF
######################################################################
Init Step ${1}/${STEP_CNT} [${2}] -- ${3}
######################################################################
EOF
}

ADMIN_PASSWORD="${ADMIN_PASSWORD:-admin}"

# Initialize the database
echo_step "1" "Starting" "Applying DB migrations"
superset db upgrade
echo_step "1" "Complete" "Applying DB migrations"

# Create an admin user
echo_step "2" "Starting" "Setting up admin user ( admin / $ADMIN_PASSWORD )"
superset fab create-admin \
    --username admin \
    --email admin@superset.com \
    --password "$ADMIN_PASSWORD" \
    --firstname Superset \
    --lastname Admin || true
echo_step "2" "Complete" "Setting up admin user"

# Create default roles and permissions
echo_step "3" "Starting" "Setting up roles and perms"
superset init
echo_step "3" "Complete" "Setting up roles and perms"

echo "Superset initialization complete!"
