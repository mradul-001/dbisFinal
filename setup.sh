#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Define root paths based on where the script is executed
ROOT_DIR="$(pwd)"
PG_BUILD_DIR="$ROOT_DIR/pg_custom_build"
CLUSTERS_DIR="$ROOT_DIR/pg_clusters"

echo "==========================================="
echo "=== Phase 1: Building Custom PostgreSQL ==="
echo "==========================================="
cd "$ROOT_DIR/dbis-project"
# Configure without any extra flags, setting the prefix to our build dir
./configure --prefix="$PG_BUILD_DIR"
make -j4
make install

echo "================================================="
echo "=== Phase 2: Building Smart Router Extension ==="
echo "================================================="
cd "$ROOT_DIR/dbis-project/contrib/smart_router"
make clean
make
make install

echo "=================================================="
echo "=== Phase 3: Initializing Database Clusters ==="
echo "=================================================="
cd "$ROOT_DIR"
# Clean up any existing clusters if re-running the script
if [ -d "$CLUSTERS_DIR" ]; then
    echo "Stopping existing clusters if they are running..."
    "$PG_BUILD_DIR/bin/pg_ctl" stop -D "$CLUSTERS_DIR/remote" -m immediate || true
    "$PG_BUILD_DIR/bin/pg_ctl" stop -D "$CLUSTERS_DIR/local" -m immediate || true
    rm -rf "$CLUSTERS_DIR"
fi

mkdir -p "$CLUSTERS_DIR/remote"
mkdir -p "$CLUSTERS_DIR/local"

"$PG_BUILD_DIR/bin/initdb" -D "$CLUSTERS_DIR/remote"
"$PG_BUILD_DIR/bin/initdb" -D "$CLUSTERS_DIR/local"

echo "========================================="
echo "=== Phase 4: Configuring Clusters ==="
echo "========================================="
# Configure remote master
echo "port = 6000" >> "$CLUSTERS_DIR/remote/postgresql.conf"

# Configure local middleware
echo "port = 6001" >> "$CLUSTERS_DIR/local/postgresql.conf"
echo "shared_preload_libraries = 'smart_router'" >> "$CLUSTERS_DIR/local/postgresql.conf"

echo "==========================================================="
echo "=== Phase 5: Starting Remote Cluster and Seeding Data ==="
echo "==========================================================="
"$PG_BUILD_DIR/bin/pg_ctl" start -D "$CLUSTERS_DIR/remote" -l "$CLUSTERS_DIR/remote/server.log"

# Wait a few seconds to ensure the remote cluster is fully ready to accept connections
sleep 3

"$PG_BUILD_DIR/bin/createdb" -h 127.0.0.1 -p 6000 company_remote

# Create generic tables but leave them completely empty as requested
"$PG_BUILD_DIR/bin/psql" -h 127.0.0.1 -p 6000 -d company_remote -c "
CREATE TABLE student (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    major VARCHAR(100),
    enrollment_year INT
);

CREATE TABLE teacher (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    department VARCHAR(100),
    title VARCHAR(50)
);

CREATE TABLE course (
    id SERIAL PRIMARY KEY,
    course_code VARCHAR(20),
    course_name VARCHAR(100),
    credits INT
);
"

echo "==========================================="
echo "=== Phase 6: Starting Local Cluster ==="
echo "==========================================="
# This will trigger the smart_router background worker to pre-scaffold the local DB
"$PG_BUILD_DIR/bin/pg_ctl" start -D "$CLUSTERS_DIR/local" -l "$CLUSTERS_DIR/local/server.log"

echo "=============================================================="
echo "=== Phase 7: Installing Frontend and Backend Node Modules ==="
echo "=============================================================="
cd "$ROOT_DIR/pg_custom_dashboard/db-middleware-api"
npm install

cd "$ROOT_DIR/pg_custom_dashboard/db-dashboard"
npm install

echo "======================="
echo "=== Setup Complete! ==="
echo "======================="
echo "Remote Cluster running on port 6000 (Database: company_remote)"
echo "Local Cluster running on port 6001 (Database: postgres)"
echo ""
echo "To run your web apps, open two new terminal windows:"
echo "1. Run 'node server.js' in pg_custom_dashboard/db-middleware-api"
echo "2. Run 'npm run dev' in pg_custom_dashboard/db-dashboard"
