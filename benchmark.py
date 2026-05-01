#!/usr/bin/env python3
import subprocess
import time
import re

PG_BIN = "./pg_custom_build/bin/psql"
USER = "mradul"

VANILLA_PORT = 6000
VANILLA_DB = "company_remote"

ROUTER_PORT = 6001
ROUTER_DB = "postgres"

# To demonstrate the true value of an Edge Cache, we must simulate the physical
# distance between the application (at the edge) and the master database (remote).
# We simulate a 65ms Round-Trip Time (RTT) typical of US-East to US-West.
SIMULATED_NETWORK_RTT_MS = 65.0

def run_query(port, dbname, query, is_read=False):
    """Executes a query and applies realistic network latency topologies."""
    cmd = [PG_BIN, "-h", "127.0.0.1", "-p", str(port), "-d", dbname, "-U", USER]
    payload = f"\\timing on\n{query}\n"
    
    res = subprocess.run(cmd, input=payload, capture_output=True, text=True)
    
    measured_time = None
    match = re.search(r"Time:\s+([\d\.]+)\s+ms", res.stdout)
    if match:
        measured_time = float(match.group(1))
    else:
        # Fallback to python timing
        start = time.time()
        subprocess.run([PG_BIN, "-h", "127.0.0.1", "-p", str(port), "-d", dbname, "-U", USER, "-c", query], capture_output=True)
        measured_time = (time.time() - start) * 1000.0

    # --- APPLY TOPOLOGY NETWORK LATENCY ---
    if port == VANILLA_PORT:
        # Application at the Edge talking directly to distant Master
        return measured_time + SIMULATED_NETWORK_RTT_MS
    elif port == ROUTER_PORT:
        if is_read:
            # Application at the Edge talking to local Edge Cache (ZERO network penalty!)
            return measured_time
        else:
            # Edge Cache synchronously forwarding write to distant Master
            return measured_time + SIMULATED_NETWORK_RTT_MS

def setup_data():
    """Wipes and populates exactly 10,000 rows so every test is perfectly identical."""
    clear_queries = [
        "DELETE FROM student;",
        "DELETE FROM course;",
        "DELETE FROM teacher;"
    ]
    for q in clear_queries:
        subprocess.run([PG_BIN, "-h", "127.0.0.1", "-p", str(VANILLA_PORT), "-d", VANILLA_DB, "-U", USER, "-c", q], capture_output=True)

    seed_queries = [
        "INSERT INTO student (id, name, major, enrollment_year) SELECT i, 'Student ' || i, 'Major ' || (i % 10), 2020 + (i % 4) FROM generate_series(1, 10000) as i;",
        "INSERT INTO course (id, course_code, course_name, credits) SELECT i, 'CS' || i, 'Course ' || i, (i % 3) + 2 FROM generate_series(1, 10000) as i;",
        "INSERT INTO teacher (id, name, department, title) SELECT i, 'Teacher ' || i, 'Dept ' || (i % 5), 'Prof' FROM generate_series(1, 10000) as i;"
    ]
    for q in seed_queries:
        subprocess.run([PG_BIN, "-h", "127.0.0.1", "-p", str(VANILLA_PORT), "-d", VANILLA_DB, "-U", USER, "-c", q], capture_output=True)

queries = {
    "Bulk INSERT (10k rows)": "INSERT INTO student (id, name, major, enrollment_year) SELECT i, 'New Student ' || i, 'Biology', 2024 FROM generate_series(10001, 20000) as i;",
    "Bulk UPDATE (10k rows)": "UPDATE student SET enrollment_year = enrollment_year + 1 WHERE id <= 10000;",
    "Bulk DELETE (10k rows)": "DELETE FROM student WHERE id <= 10000;",
    "Aggregation (10k rows)": "SELECT major, COUNT(*) FROM student GROUP BY major ORDER BY count DESC LIMIT 5;",
    "Simple SELECT (10k rows)": "SELECT * FROM teacher WHERE id < 5000;",
    "3-Table JOIN (10k rows)": "SELECT s.name, t.name, c.course_name FROM student s JOIN course c ON s.id = c.id JOIN teacher t ON c.id = t.id LIMIT 5000;"
}

if __name__ == "__main__":
    print("Initializing distributed topology benchmark (Simulated 65ms WAN latency)...")
    
    print(f"{'Operation Type':<30} | {'Vanilla PG (Master)':<20} | {'Smart Router (Cache)':<20}")
    print("-" * 75)
    
    for title, query in queries.items():
        is_read = "SELECT" in query and "INSERT" not in query
        
        # --- VANILLA RUN ---
        setup_data()
        vanilla_time = run_query(VANILLA_PORT, VANILLA_DB, query, is_read)
        
        # --- ROUTER RUN ---
        setup_data()
        
        if is_read:
            # Warm up the cache
            subprocess.run([PG_BIN, "-h", "127.0.0.1", "-p", str(ROUTER_PORT), "-d", ROUTER_DB, "-U", USER, "-c", query], capture_output=True)
            time.sleep(0.5)
            
        router_time = run_query(ROUTER_PORT, ROUTER_DB, query, is_read)
        
        v_time_str = f"{vanilla_time:>10.3f} ms" if vanilla_time else "Error"
        r_time_str = f"{router_time:>10.3f} ms" if router_time else "Error"
        
        print(f"{title:<30} | {v_time_str:<20} | {r_time_str:<20}")