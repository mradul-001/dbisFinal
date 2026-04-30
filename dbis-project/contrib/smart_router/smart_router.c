#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "tcop/tcopprot.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "utils/guc.h"
#include "libpq-fe.h"
#include "executor/spi.h"
#include "executor/executor.h"
#include "utils/snapmgr.h"
#include "access/xact.h"

PG_MODULE_MAGIC;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;

void _PG_init(void);
PGDLLEXPORT void smart_router_main(Datum main_arg);
static void smart_router_ExecutorStart(QueryDesc *queryDesc, int eflags);

static bool in_hook = false;

// ------------------------------------------------------------------
// ----------------------- LRU Cache Tracking -----------------------
#define CACHE_MAX_TABLES 5

typedef struct {
    char table_name[64];
    long last_used;
} LRUEntry;

static LRUEntry lru_cache[CACHE_MAX_TABLES];
static int      lru_count = 0;
static long     lru_clock = 0;

static int lru_find(const char *table_name)
{
    for (int i = 0; i < lru_count; i++)
        if (strcmp(lru_cache[i].table_name, table_name) == 0)
            return i;
    return -1;
}

static void lru_touch(int idx)
{
    lru_cache[idx].last_used = ++lru_clock;
}

/* Evicts the least recently used table: deletes its rows and removes it from the tracker */
static void lru_evict(void)
{
    int min_idx = 0;
    for (int i = 1; i < lru_count; i++)
        if (lru_cache[i].last_used < lru_cache[min_idx].last_used)
            min_idx = i;

    char evict_sql[128];
    snprintf(evict_sql, sizeof(evict_sql), "DELETE FROM %s", lru_cache[min_idx].table_name);
    elog(LOG, "Smart Router: LRU full. Evicting table '%s' (last_used=%ld).",
         lru_cache[min_idx].table_name, lru_cache[min_idx].last_used);
    SPI_execute(evict_sql, false, 0);

    /* Compact the array */
    lru_cache[min_idx] = lru_cache[lru_count - 1];
    lru_count--;
}

static void lru_add(const char *table_name)
{
    if (lru_count == CACHE_MAX_TABLES)
        lru_evict();

    strncpy(lru_cache[lru_count].table_name, table_name, 63);
    lru_cache[lru_count].table_name[63] = '\0';
    lru_cache[lru_count].last_used = ++lru_clock;
    lru_count++;
    elog(LOG, "Smart Router: '%s' added to LRU cache. [%d/%d slots used]",
         table_name, lru_count, CACHE_MAX_TABLES);
}
// ------------------------------------------------------------------

static void smart_router_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
    if (!in_hook && queryDesc->sourceText)
    {
        const char *query_text = queryDesc->sourceText;
        bool is_select = pg_strncasecmp(query_text, "SELECT", 6) == 0;
        bool is_insert = pg_strncasecmp(query_text, "INSERT", 6) == 0;
        bool is_update = pg_strncasecmp(query_text, "UPDATE", 6) == 0;
        bool is_delete = pg_strncasecmp(query_text, "DELETE", 6) == 0;

        // ==============================================================
        // ======================== SELECT HANDLER ======================
        // ==============================================================
        if (is_select)
        {
            in_hook = true;
            elog(LOG, "Smart Router: SELECT intercepted: %s", query_text);

            char table_name[64] = {0};
            const char *from_ptr = strstr(query_text, "FROM ");
            if (!from_ptr) from_ptr = strstr(query_text, "from ");
            if (!from_ptr) from_ptr = strstr(query_text, "From ");

            if (from_ptr)
            {
                from_ptr += 5; // Skip "FROM "
                while (*from_ptr == ' ' || *from_ptr == '\t' || *from_ptr == '\n') from_ptr++;

                int i = 0;
                while (from_ptr[i] != '\0' && from_ptr[i] != ' ' && from_ptr[i] != ';' &&
                       from_ptr[i] != '\n' && from_ptr[i] != '\t' && i < 63)
                {
                    char c = from_ptr[i];
                    if (c >= 'A' && c <= 'Z') c = c + ('a' - 'A');
                    table_name[i] = c;
                    i++;
                }
                table_name[i] = '\0';
            }

            if (table_name[0] != '\0')
            {
                SPI_connect();

                int lru_idx = lru_find(table_name);

                if (lru_idx >= 0)
                {
                    /* ---- CACHE HIT (tracked in LRU) ---- */
                    lru_touch(lru_idx);
                    elog(LOG, "Smart Router: LRU cache hit for '%s'. [last_used refreshed to %ld]",
                         table_name, lru_cache[lru_idx].last_used);
                }
                else
                {
                    /*
                     * Table not in LRU tracker.
                     * Check if the local table already has rows
                     * (e.g. data survived from a previous session after restart).
                     */
                    char check_q[256];
                    snprintf(check_q, sizeof(check_q), "SELECT count(*) FROM %s", table_name);
                    int ret = SPI_execute(check_q, true, 0);

                    bool is_empty = true;
                    if (ret == SPI_OK_SELECT && SPI_processed > 0 && SPI_tuptable != NULL)
                    {
                        char *val = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
                        if (val && strcmp(val, "0") != 0)
                            is_empty = false;
                    }

                    if (!is_empty)
                    {
                        /* Local rows exist but weren't tracked (e.g. after restart) — re-register */
                        elog(LOG, "Smart Router: '%s' has local data but wasn't in LRU. Re-registering.",
                             table_name);
                        lru_add(table_name);
                    }
                    else
                    {
                        /* ---- CACHE MISS: fetch from remote, populate local, register in LRU ---- */
                        elog(LOG, "Smart Router: Cache miss for '%s'. Fetching from remote...", table_name);

                        PGconn *conn = PQconnectdb("host=127.0.0.1 port=6000 dbname=company_remote user=mradul");
                        if (PQstatus(conn) == CONNECTION_OK)
                        {
                            char fetch_all_query[256];
                            snprintf(fetch_all_query, sizeof(fetch_all_query), "SELECT * FROM %s", table_name);
                            PGresult *res = PQexec(conn, fetch_all_query);

                            if (PQresultStatus(res) == PGRES_TUPLES_OK)
                            {
                                int rows = PQntuples(res);
                                int cols = PQnfields(res);

                                for (int i = 0; i < rows; i++)
                                {
                                    StringInfoData insert_sql;
                                    initStringInfo(&insert_sql);
                                    appendStringInfo(&insert_sql, "INSERT INTO %s VALUES (", table_name);

                                    for (int j = 0; j < cols; j++)
                                    {
                                        char *val = PQgetvalue(res, i, j);
                                        if (j > 0) appendStringInfoString(&insert_sql, ", ");
                                        appendStringInfo(&insert_sql, "'%s'", val);
                                    }
                                    appendStringInfoString(&insert_sql, ")");
                                    SPI_execute(insert_sql.data, false, 0);
                                }
                                elog(LOG, "Smart Router: Cached %d rows for '%s'.", rows, table_name);

                                /* Register the newly populated table in LRU */
                                lru_add(table_name);

                                /* Make inserted rows visible to the current query */
                                CommandCounterIncrement();
                                if (queryDesc->snapshot)
                                    queryDesc->snapshot->curcid = GetCurrentCommandId(false);
                            }
                            PQclear(res);
                        }
                        else
                        {
                            elog(WARNING, "Smart Router: Could not connect to remote for cache miss on '%s': %s",
                                 table_name, PQerrorMessage(conn));
                        }
                        PQfinish(conn);
                    }
                }

                SPI_finish();
            }

            in_hook = false;
        }

        // ==============================================================
        // ======================== INSERT HANDLER ======================
        // ==============================================================
        else if (is_insert)
        {
            in_hook = true;
            elog(LOG, "Smart Router: INSERT intercepted: %s", query_text);

            /* --- Parse table name from "INSERT INTO <table>" --- */
            char table_name[64] = {0};
            const char *into_ptr = strcasestr(query_text, "INTO ");
            if (into_ptr)
            {
                into_ptr += 5;
                while (*into_ptr == ' ' || *into_ptr == '\t') into_ptr++;

                int i = 0;
                while (into_ptr[i] != '\0' && into_ptr[i] != ' ' &&
                       into_ptr[i] != '(' && into_ptr[i] != '\n' &&
                       into_ptr[i] != '\t' && i < 63)
                {
                    char c = into_ptr[i];
                    if (c >= 'A' && c <= 'Z') c = c + ('a' - 'A');
                    table_name[i] = c;
                    i++;
                }
                table_name[i] = '\0';
            }

            if (table_name[0] != '\0')
            {
                elog(LOG, "Smart Router: INSERT targeting table '%s'. Writing to remote first...",
                     table_name);

                /* --- Step 1: Write to remote (source of truth) --- */
                PGconn *conn = PQconnectdb(
                    "host=127.0.0.1 port=6000 dbname=company_remote user=mradul");

                if (PQstatus(conn) != CONNECTION_OK)
                {
                    /*
                     * Remote is unreachable — abort entirely.
                     * We must NOT allow a local insert that would make
                     * the cache inconsistent with the remote master.
                     */
                    elog(ERROR,
                         "Smart Router: Remote connection failed during INSERT on '%s': %s. "
                         "Aborting to preserve consistency.",
                         table_name, PQerrorMessage(conn));
                    PQfinish(conn);
                    in_hook = false;
                    ereport(ERROR,
                            (errcode(ERRCODE_CONNECTION_FAILURE),
                             errmsg("Smart Router: Could not reach remote master. INSERT aborted.")));
                }

                PGresult *res = PQexec(conn, query_text);
                ExecStatusType status = PQresultStatus(res);

                if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK)
                {
                    /*
                     * Remote rejected the insert (constraint violation, type mismatch, etc.)
                     * Suppress the local insert to keep both sides in sync.
                     */
                    elog(ERROR,
                         "Smart Router: Remote INSERT failed for '%s': %s. "
                         "Suppressing local insert.",
                         table_name, PQresultErrorMessage(res));
                    PQclear(res);
                    PQfinish(conn);
                    in_hook = false;
                    ereport(ERROR,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Smart Router: Remote INSERT failed. Local cache not modified.")));
                }

                elog(LOG, "Smart Router: Remote INSERT succeeded for '%s'. Updating LRU and proceeding locally...",
                     table_name);
                PQclear(res);
                PQfinish(conn);

                /* --- Step 2: Update LRU tracking --- */
                SPI_connect();

                int lru_idx = lru_find(table_name);
                if (lru_idx >= 0)
                {
                    /*
                     * Table already in local cache — just refresh its recency.
                     * The actual local row insert happens via the standard hook chain below.
                     */
                    lru_touch(lru_idx);
                    elog(LOG, "Smart Router: LRU touched for '%s' after INSERT. [%d/%d slots used]",
                         table_name, lru_count, CACHE_MAX_TABLES);
                }
                else
                {
                    /*
                     * Table not yet tracked in LRU — this INSERT naturally
                     * warms up the cache for future SELECTs on this table.
                     */
                    elog(LOG, "Smart Router: '%s' not in LRU. Registering after INSERT warm-up.",
                         table_name);
                    lru_add(table_name);
                }

                SPI_finish();

                /* Step 3: Fall through to standard execution below — local insert proceeds normally */
                elog(LOG, "Smart Router: Proceeding with local INSERT for '%s'.", table_name);
            }

            in_hook = false;
        }

        // ==============================================================
        // ======================== UPDATE HANDLER ======================
        // ==============================================================
        else if (is_update)
        {
            in_hook = true;
            elog(LOG, "Smart Router: UPDATE intercepted: %s", query_text);

            /* --- Parse table name from "UPDATE <table> SET" --- */
            char table_name[64] = {0};
            const char *update_ptr = strcasestr(query_text, "UPDATE ");
            if (update_ptr)
            {
                update_ptr += 7;
                while (*update_ptr == ' ' || *update_ptr == '\t') update_ptr++;

                int i = 0;
                while (update_ptr[i] != '\0' && update_ptr[i] != ' ' &&
                       update_ptr[i] != '\n'  && update_ptr[i] != '\t' && i < 63)
                {
                    char c = update_ptr[i];
                    if (c >= 'A' && c <= 'Z') c = c + ('a' - 'A');
                    table_name[i] = c;
                    i++;
                }
                table_name[i] = '\0';
            }

            if (table_name[0] != '\0')
            {
                elog(LOG, "Smart Router: UPDATE targeting table '%s'. Writing to remote first...",
                     table_name);

                /* --- Step 1: Execute on remote first --- */
                PGconn *conn = PQconnectdb(
                    "host=127.0.0.1 port=6000 dbname=company_remote user=mradul");

                if (PQstatus(conn) != CONNECTION_OK)
                {
                    elog(ERROR,
                         "Smart Router: Remote connection failed during UPDATE on '%s': %s. "
                         "Aborting to preserve consistency.",
                         table_name, PQerrorMessage(conn));
                    PQfinish(conn);
                    in_hook = false;
                    ereport(ERROR,
                            (errcode(ERRCODE_CONNECTION_FAILURE),
                             errmsg("Smart Router: Could not reach remote master. UPDATE aborted.")));
                }

                PGresult *res = PQexec(conn, query_text);
                ExecStatusType status = PQresultStatus(res);

                if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK)
                {
                    /*
                     * Remote rejected the update — suppress local execution
                     * so both sides remain consistent.
                     */
                    elog(ERROR,
                         "Smart Router: Remote UPDATE failed for '%s': %s. "
                         "Suppressing local update.",
                         table_name, PQresultErrorMessage(res));
                    PQclear(res);
                    PQfinish(conn);
                    in_hook = false;
                    ereport(ERROR,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Smart Router: Remote UPDATE failed. Local cache not modified.")));
                }

                elog(LOG, "Smart Router: Remote UPDATE succeeded for '%s'. Proceeding with local update...",
                     table_name);
                PQclear(res);
                PQfinish(conn);

                /* --- Step 2: Update LRU tracking ---
                 * UPDATE implies the table is actively used — touch if cached,
                 * but do NOT auto-add if not cached (UPDATE alone is not a warm-up). */
                SPI_connect();
                int lru_idx = lru_find(table_name);
                if (lru_idx >= 0)
                {
                    lru_touch(lru_idx);
                    elog(LOG, "Smart Router: LRU touched for '%s' after UPDATE. [%d/%d slots used]",
                         table_name, lru_count, CACHE_MAX_TABLES);
                }
                else
                {
                    /*
                     * Table not in local cache — local UPDATE will apply to an
                     * empty table which is harmless (0 rows affected).
                     * We still let it fall through so PG doesn't error out.
                     */
                    elog(LOG, "Smart Router: '%s' not in LRU cache. Local UPDATE will be a no-op on empty table.",
                         table_name);
                }
                SPI_finish();

                /* Step 3: Fall through — local UPDATE runs via standard hook chain */
                elog(LOG, "Smart Router: Proceeding with local UPDATE for '%s'.", table_name);
            }

            in_hook = false;
        }

        // ==============================================================
        // ======================== DELETE HANDLER ======================
        // ==============================================================
        else if (is_delete)
        {
            in_hook = true;
            elog(LOG, "Smart Router: DELETE intercepted: %s", query_text);

            /* --- Parse table name from "DELETE FROM <table>" --- */
            char table_name[64] = {0};
            const char *from_ptr = strcasestr(query_text, "FROM ");
            if (from_ptr)
            {
                from_ptr += 5;
                while (*from_ptr == ' ' || *from_ptr == '\t') from_ptr++;

                int i = 0;
                while (from_ptr[i] != '\0' && from_ptr[i] != ' ' &&
                       from_ptr[i] != '\n'  && from_ptr[i] != '\t' &&
                       from_ptr[i] != ';'   && i < 63)
                {
                    char c = from_ptr[i];
                    if (c >= 'A' && c <= 'Z') c = c + ('a' - 'A');
                    table_name[i] = c;
                    i++;
                }
                table_name[i] = '\0';
            }

            if (table_name[0] != '\0')
            {
                elog(LOG, "Smart Router: DELETE targeting table '%s'. Writing to remote first...",
                     table_name);

                /* --- Step 1: Execute on remote first --- */
                PGconn *conn = PQconnectdb(
                    "host=127.0.0.1 port=6000 dbname=company_remote user=mradul");

                if (PQstatus(conn) != CONNECTION_OK)
                {
                    elog(ERROR,
                         "Smart Router: Remote connection failed during DELETE on '%s': %s. "
                         "Aborting to preserve consistency.",
                         table_name, PQerrorMessage(conn));
                    PQfinish(conn);
                    in_hook = false;
                    ereport(ERROR,
                            (errcode(ERRCODE_CONNECTION_FAILURE),
                             errmsg("Smart Router: Could not reach remote master. DELETE aborted.")));
                }

                PGresult *res = PQexec(conn, query_text);
                ExecStatusType status = PQresultStatus(res);

                if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK)
                {
                    /*
                     * Remote rejected the delete — suppress local execution
                     * so both sides remain consistent.
                     */
                    elog(ERROR,
                         "Smart Router: Remote DELETE failed for '%s': %s. "
                         "Suppressing local delete.",
                         table_name, PQresultErrorMessage(res));
                    PQclear(res);
                    PQfinish(conn);
                    in_hook = false;
                    ereport(ERROR,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Smart Router: Remote DELETE failed. Local cache not modified.")));
                }

                elog(LOG, "Smart Router: Remote DELETE succeeded for '%s'. Proceeding with local delete...",
                     table_name);
                PQclear(res);
                PQfinish(conn);

                /* --- Step 2: Update LRU tracking ---
                 * If this is a full-table DELETE (no WHERE clause), the table
                 * is now empty — remove it from LRU so the next SELECT re-fetches
                 * cleanly. If it's a partial DELETE (has WHERE), just touch it. */
                SPI_connect();
                int lru_idx = lru_find(table_name);

                bool is_full_delete = (strcasestr(query_text, "WHERE") == NULL);

                if (lru_idx >= 0)
                {
                    if (is_full_delete)
                    {
                        /*
                         * Entire table wiped — evict from LRU so the next SELECT
                         * triggers a fresh fetch from remote instead of seeing an
                         * empty local table.
                         */
                        elog(LOG, "Smart Router: Full DELETE on '%s'. Removing from LRU cache. [%d/%d slots used]",
                             table_name, lru_count - 1, CACHE_MAX_TABLES);
                        lru_cache[lru_idx] = lru_cache[lru_count - 1];
                        lru_count--;
                    }
                    else
                    {
                        /* Partial DELETE — table still has rows, just refresh recency */
                        lru_touch(lru_idx);
                        elog(LOG, "Smart Router: Partial DELETE on '%s'. LRU touched. [%d/%d slots used]",
                             table_name, lru_count, CACHE_MAX_TABLES);
                    }
                }
                else
                {
                    elog(LOG, "Smart Router: '%s' not in LRU cache. Local DELETE will be a no-op on empty table.",
                         table_name);
                }
                SPI_finish();

                /* Step 3: Fall through — local DELETE runs via standard hook chain */
                elog(LOG, "Smart Router: Proceeding with local DELETE for '%s'.", table_name);
            }

            in_hook = false;
        }
    }

    if (prev_ExecutorStart)
        prev_ExecutorStart(queryDesc, eflags);
    else
        standard_ExecutorStart(queryDesc, eflags);
}


void _PG_init(void)
{
    BackgroundWorker worker;

    MemSet(&worker, 0, sizeof(BackgroundWorker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    sprintf(worker.bgw_library_name, "smart_router");
    sprintf(worker.bgw_function_name, "smart_router_main");
    sprintf(worker.bgw_name, "Smart Router Schema Synchronizer");
    sprintf(worker.bgw_type, "smart_router");
    worker.bgw_main_arg = (Datum)0;

    RegisterBackgroundWorker(&worker);

    // Register hooks
    prev_ExecutorStart = ExecutorStart_hook;
    ExecutorStart_hook = smart_router_ExecutorStart;
}

void smart_router_main(Datum main_arg)
{
    // Establish signals to handle shutdown gracefully
    pqsignal(SIGTERM, die);
    BackgroundWorkerUnblockSignals();

    // Connect to the local database where the extension is running
    BackgroundWorkerInitializeConnection("postgres", NULL, 0);
    elog(LOG, "Smart Router Schema Synchronizer started successfully inside contrib!");

    // --- NEW PHASE 2 CODE ---
    PGconn *conn;

    // Define the connection string to our Remote Master
    const char *conninfo = "host=127.0.0.1 port=6000 dbname=company_remote user=mradul";
    elog(LOG, "Smart Router: Attempting to connect to remote master at port 6000...");

    // 1. Establish the connection
    conn = PQconnectdb(conninfo);

    // 2. Check to see that the backend connection was successfully made
    if (PQstatus(conn) != CONNECTION_OK)
    {
        elog(ERROR, "Smart Router: Connection to remote database failed: %s", PQerrorMessage(conn));
        PQfinish(conn);
        proc_exit(1);
    }
    elog(LOG, "Smart Router: Successfully connected to remote master! Fetching schema...");

    // 3. Fetch schema information from remote
    const char *query = "SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema='public' ORDER BY table_name, ordinal_position";
    PGresult *res = PQexec(conn, query);

    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        elog(ERROR, "Smart Router: Schema query failed: %s", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        proc_exit(1);
    }

    int rows = PQntuples(res);
    if (rows > 0)
    {
        // Start a local transaction and connect to SPI
        SetCurrentStatementStartTimestamp();
        StartTransactionCommand();
        SPI_connect();
        PushActiveSnapshot(GetTransactionSnapshot());

        char current_table[256] = "";
        StringInfoData create_sql;
        initStringInfo(&create_sql);

        for (int i = 0; i < rows; i++)
        {
            char *table_name = PQgetvalue(res, i, 0);
            char *column_name = PQgetvalue(res, i, 1);
            char *data_type = PQgetvalue(res, i, 2);

            if (strcmp(current_table, table_name) != 0)
            {
                // If we were building a table, finish it and execute
                if (current_table[0] != '\0')
                {
                    appendStringInfoString(&create_sql, ");");
                    elog(LOG, "Smart Router: Executing %s", create_sql.data);
                    SPI_execute(create_sql.data, false, 0);
                }

                // Start new table
                strcpy(current_table, table_name);
                resetStringInfo(&create_sql);
                appendStringInfo(&create_sql, "CREATE TABLE IF NOT EXISTS %s (", table_name);
                appendStringInfo(&create_sql, "%s %s", column_name, data_type);
            }
            else
            {
                // Continue adding columns
                appendStringInfo(&create_sql, ", %s %s", column_name, data_type);
            }
        }

        // Execute the last table
        if (current_table[0] != '\0')
        {
            appendStringInfoString(&create_sql, ");");
            elog(LOG, "Smart Router: Executing %s", create_sql.data);
            SPI_execute(create_sql.data, false, 0);
        }

        // Finish SPI and transaction
        PopActiveSnapshot();
        SPI_finish();
        CommitTransactionCommand();
    }

    PQclear(res);
    // Clean up the connection before exiting
    PQfinish(conn);
    elog(LOG, "Smart Router Schema Synchronizer finished its initial sync.");
    proc_exit(0);
}