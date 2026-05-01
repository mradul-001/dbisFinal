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


// ------------------------------------------------------------------
// -------------------- JOIN Helper Functions -----------------------

#define MAX_JOIN_TABLES 16

static int extract_join_tables(const char *query_text, char tables[][64], int max_tables)
{
    int count = 0;
    const char *ptr = query_text;

    while (*ptr != '\0' && count < max_tables)
    {
        int skip = 0;

        if      (pg_strncasecmp(ptr, "FROM ", 5) == 0) skip = 5;
        else if (pg_strncasecmp(ptr, "JOIN ", 5) == 0) skip = 5;

        if (skip > 0)
        {
            const char *tbl_ptr = ptr + skip;
            while (*tbl_ptr == ' ' || *tbl_ptr == '\t' || *tbl_ptr == '\n') tbl_ptr++;

            int i = 0;
            while (tbl_ptr[i] != '\0' && tbl_ptr[i] != ' ' && tbl_ptr[i] != '\t' &&
                   tbl_ptr[i] != '\n'  && tbl_ptr[i] != '(' && tbl_ptr[i] != ',' &&
                   tbl_ptr[i] != ';'   && i < 63)
            {
                char c = tbl_ptr[i];
                if (c >= 'A' && c <= 'Z') c = c + ('a' - 'A');
                tables[count][i] = c;
                i++;
            }
            tables[count][i] = '\0';

            if (tables[count][0] != '\0'                    &&
                pg_strcasecmp(tables[count], "select") != 0 &&
                pg_strcasecmp(tables[count], "on")     != 0 &&
                pg_strcasecmp(tables[count], "where")  != 0 &&
                pg_strcasecmp(tables[count], "set")    != 0)
            {
                count++;
            }
            ptr = tbl_ptr;
        }
        else
        {
            ptr++;
        }
    }
    return count;
}

static bool fetch_table_into_cache(const char *table_name)
{
    PGconn *conn = PQconnectdb(
        "host=127.0.0.1 port=6000 dbname=company_remote user=mradul");

    if (PQstatus(conn) != CONNECTION_OK)
    {
        elog(WARNING, "Smart Router JOIN: Could not connect to remote for table '%s': %s",
             table_name, PQerrorMessage(conn));
        PQfinish(conn);
        return false;
    }

    char fetch_q[256];
    snprintf(fetch_q, sizeof(fetch_q), "SELECT * FROM %s", table_name);
    PGresult *res = PQexec(conn, fetch_q);

    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        elog(WARNING, "Smart Router JOIN: Remote fetch failed for '%s': %s",
             table_name, PQresultErrorMessage(res));
        PQclear(res);
        PQfinish(conn);
        return false;
    }

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

    elog(LOG, "Smart Router JOIN: Fetched and cached %d rows for '%s'.", rows, table_name);
    PQclear(res);
    PQfinish(conn);

    lru_add(table_name);   /* evicts oldest entry automatically if cache is full */
    return true;
}

static bool run_join_on_remote(const char *query_text, QueryDesc *queryDesc)
{
    elog(LOG, "Smart Router JOIN OVERFLOW: Executing full JOIN on remote master...");

    PGconn *conn = PQconnectdb(
        "host=127.0.0.1 port=6000 dbname=company_remote user=mradul");

    if (PQstatus(conn) != CONNECTION_OK)
    {
        elog(WARNING, "Smart Router JOIN OVERFLOW: Remote connection failed: %s",
             PQerrorMessage(conn));
        PQfinish(conn);
        return false;
    }

    PGresult *res = PQexec(conn, query_text);

    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        elog(WARNING, "Smart Router JOIN OVERFLOW: Remote query failed: %s",
             PQresultErrorMessage(res));
        PQclear(res);
        PQfinish(conn);
        return false;
    }

    int rows = PQntuples(res);
    int cols = PQnfields(res);
    elog(LOG, "Smart Router JOIN OVERFLOW: Remote returned %d rows across %d columns. "
              "Staging into _sr_remote_result...", rows, cols);

    /*
     * Build a temp table whose columns match the remote result set.
     */
    StringInfoData create_sql;
    initStringInfo(&create_sql);
    appendStringInfoString(&create_sql,
        "CREATE TEMP TABLE IF NOT EXISTS _sr_remote_result (");
    for (int j = 0; j < cols; j++)
    {
        if (j > 0) appendStringInfoString(&create_sql, ", ");
        appendStringInfo(&create_sql, "%s TEXT", PQfname(res, j));
    }
    appendStringInfoString(&create_sql, ")");
    SPI_execute(create_sql.data, false, 0);

    SPI_execute("DELETE FROM _sr_remote_result", false, 0);

    /* Stage all remote rows locally */
    for (int i = 0; i < rows; i++)
    {
        StringInfoData insert_sql;
        initStringInfo(&insert_sql);
        appendStringInfoString(&insert_sql, "INSERT INTO _sr_remote_result VALUES (");
        for (int j = 0; j < cols; j++)
        {
            if (j > 0) appendStringInfoString(&insert_sql, ", ");
            appendStringInfo(&insert_sql, "'%s'", PQgetvalue(res, i, j));
        }
        appendStringInfoString(&insert_sql, ")");
        SPI_execute(insert_sql.data, false, 0);
    }

    /* Make staged rows visible within this transaction */
    CommandCounterIncrement();
    if (queryDesc->snapshot)
        queryDesc->snapshot->curcid = GetCurrentCommandId(false);

    elog(LOG, "Smart Router JOIN OVERFLOW: %d rows staged in _sr_remote_result.", rows);

    PQclear(res);
    PQfinish(conn);
    return true;
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

            bool is_join = (strcasestr(query_text, " JOIN ") != NULL);

            if (is_join)
            {
                char join_tables[MAX_JOIN_TABLES][64];
                int  table_count = extract_join_tables(
                    query_text, join_tables, MAX_JOIN_TABLES);

                elog(LOG, "Smart Router JOIN: Detected %d table(s). Cache capacity: %d.",
                     table_count, CACHE_MAX_TABLES);

                SPI_connect();

                // OVERFLOW CHECK
                if (table_count > CACHE_MAX_TABLES)
                {
                    elog(WARNING,
                         "Smart Router JOIN OVERFLOW: Query spans %d tables but "
                         "CACHE_MAX_TABLES=%d. Cannot cache all tables simultaneously. "
                         "Routing entire JOIN to remote master.",
                         table_count, CACHE_MAX_TABLES);

                    bool remote_ok = run_join_on_remote(query_text, queryDesc);

                    if (remote_ok)
                        elog(LOG,
                             "Smart Router JOIN OVERFLOW: Success. "
                             "Retrieve results with: SELECT * FROM _sr_remote_result;");
                    else
                        elog(WARNING,
                             "Smart Router JOIN OVERFLOW: Remote execution failed. "
                             "Query results will be empty.");

                    //  LRU is intentionally untouched — overflow tables are NOT
                    //  being cached, so cache state must stay consistent.
                    SPI_finish();
                    in_hook = false;
                }
                else
                {
                    // Normal JOIN (table_count <= CACHE_MAX_TABLES):
                    bool all_ok = true;

                    for (int t = 0; t < table_count; t++)
                    {
                        const char *tname = join_tables[t];
                        int lru_idx = lru_find(tname);

                        if (lru_idx >= 0)
                        {
                            lru_touch(lru_idx);
                            elog(LOG, "Smart Router JOIN: Cache hit for '%s'. LRU refreshed.",
                                 tname);
                        }
                        else
                        {
                            char check_q[256];
                            snprintf(check_q, sizeof(check_q),
                                     "SELECT count(*) FROM %s", tname);
                            int ret = SPI_execute(check_q, true, 0);

                            bool is_empty = true;
                            if (ret == SPI_OK_SELECT && SPI_processed > 0 &&
                                SPI_tuptable != NULL)
                            {
                                char *val = SPI_getvalue(
                                    SPI_tuptable->vals[0],
                                    SPI_tuptable->tupdesc, 1);
                                if (val && strcmp(val, "0") != 0) is_empty = false;
                            }

                            if (!is_empty)
                            {
                                elog(LOG,
                                     "Smart Router JOIN: '%s' has local data. Re-registering.",
                                     tname);
                                lru_add(tname);
                            }
                            else
                            {
                                elog(LOG,
                                     "Smart Router JOIN: Cache miss for '%s'. Fetching...",
                                     tname);
                                if (!fetch_table_into_cache(tname))
                                {
                                    elog(WARNING,
                                         "Smart Router JOIN: Failed to cache '%s'. "
                                         "JOIN may return incomplete results.", tname);
                                    all_ok = false;
                                }
                            }
                        }
                    }

                    if (all_ok)
                    {
                        CommandCounterIncrement();
                        if (queryDesc->snapshot)
                            queryDesc->snapshot->curcid = GetCurrentCommandId(false);
                        elog(LOG,
                             "Smart Router JOIN: All %d table(s) cached. Running locally.",
                             table_count);
                    }
                    else
                    {
                        elog(WARNING,
                             "Smart Router JOIN: One or more tables failed to cache. "
                             "Results may be partial.");
                    }

                    SPI_finish();
                    in_hook = false;
                }
            }
            else
            {
                // ------------------------------------------------------
                // Single-table SELECT path (unchanged)
                // ------------------------------------------------------
                char table_name[64] = {0};
                const char *from_ptr = strstr(query_text, "FROM ");
                if (!from_ptr) from_ptr = strstr(query_text, "from ");
                if (!from_ptr) from_ptr = strstr(query_text, "From ");

                if (from_ptr)
                {
                    from_ptr += 5;
                    while (*from_ptr == ' ' || *from_ptr == '\t' || *from_ptr == '\n')
                        from_ptr++;

                    int i = 0;
                    while (from_ptr[i] != '\0' && from_ptr[i] != ' ' &&
                           from_ptr[i] != ';'   && from_ptr[i] != '\n' &&
                           from_ptr[i] != '\t'  && i < 63)
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
                        lru_touch(lru_idx);
                        elog(LOG,
                             "Smart Router: LRU cache hit for '%s'. [last_used=%ld]",
                             table_name, lru_cache[lru_idx].last_used);
                    }
                    else
                    {
                        char check_q[256];
                        snprintf(check_q, sizeof(check_q),
                                 "SELECT count(*) FROM %s", table_name);
                        int ret = SPI_execute(check_q, true, 0);

                        bool is_empty = true;
                        if (ret == SPI_OK_SELECT && SPI_processed > 0 &&
                            SPI_tuptable != NULL)
                        {
                            char *val = SPI_getvalue(
                                SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
                            if (val && strcmp(val, "0") != 0) is_empty = false;
                        }

                        if (!is_empty)
                        {
                            elog(LOG,
                                 "Smart Router: '%s' has local data but not in LRU. "
                                 "Re-registering.", table_name);
                            lru_add(table_name);
                        }
                        else
                        {
                            elog(LOG,
                                 "Smart Router: Cache miss for '%s'. Fetching from remote...",
                                 table_name);

                            PGconn *conn = PQconnectdb(
                                "host=127.0.0.1 port=6000 dbname=company_remote user=mradul");

                            if (PQstatus(conn) == CONNECTION_OK)
                            {
                                char fetch_all_query[256];
                                snprintf(fetch_all_query, sizeof(fetch_all_query),
                                         "SELECT * FROM %s", table_name);
                                PGresult *res = PQexec(conn, fetch_all_query);

                                if (PQresultStatus(res) == PGRES_TUPLES_OK)
                                {
                                    int rows = PQntuples(res);
                                    int cols = PQnfields(res);

                                    for (int i = 0; i < rows; i++)
                                    {
                                        StringInfoData insert_sql;
                                        initStringInfo(&insert_sql);
                                        appendStringInfo(&insert_sql,
                                            "INSERT INTO %s VALUES (", table_name);
                                        for (int j = 0; j < cols; j++)
                                        {
                                            char *val = PQgetvalue(res, i, j);
                                            if (j > 0)
                                                appendStringInfoString(&insert_sql, ", ");
                                            appendStringInfo(&insert_sql, "'%s'", val);
                                        }
                                        appendStringInfoString(&insert_sql, ")");
                                        SPI_execute(insert_sql.data, false, 0);
                                    }
                                    elog(LOG, "Smart Router: Cached %d rows for '%s'.",
                                         rows, table_name);
                                    lru_add(table_name);
                                    CommandCounterIncrement();
                                    if (queryDesc->snapshot)
                                        queryDesc->snapshot->curcid =
                                            GetCurrentCommandId(false);
                                }
                                PQclear(res);
                            }
                            else
                            {
                                elog(WARNING,
                                     "Smart Router: Could not connect to remote for '%s': %s",
                                     table_name, PQerrorMessage(conn));
                            }
                            PQfinish(conn);
                        }
                    }
                    SPI_finish();
                }

                in_hook = false;
            }
        }

        // ==============================================================
        // ======================== INSERT HANDLER ======================
        // ==============================================================
        else if (is_insert)
        {
            in_hook = true;
            elog(LOG, "Smart Router: INSERT intercepted: %s", query_text);

            char table_name[64] = {0};
            const char *into_ptr = strcasestr(query_text, "INTO ");
            if (into_ptr)
            {
                into_ptr += 5;
                while (*into_ptr == ' ' || *into_ptr == '\t') into_ptr++;

                int i = 0;
                while (into_ptr[i] != '\0' && into_ptr[i] != ' ' &&
                       into_ptr[i] != '('   && into_ptr[i] != '\n' &&
                       into_ptr[i] != '\t'  && i < 63)
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
                elog(LOG, "Smart Router: INSERT targeting '%s'. Writing to remote first...",
                     table_name);

                PGconn *conn = PQconnectdb(
                    "host=127.0.0.1 port=6000 dbname=company_remote user=mradul");

                if (PQstatus(conn) != CONNECTION_OK)
                {
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
                    elog(ERROR, "Smart Router: Remote INSERT failed for '%s': %s.",
                         table_name, PQresultErrorMessage(res));
                    PQclear(res);
                    PQfinish(conn);
                    in_hook = false;
                    ereport(ERROR,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Smart Router: Remote INSERT failed. Local cache not modified.")));
                }

                elog(LOG, "Smart Router: Remote INSERT succeeded for '%s'.", table_name);
                PQclear(res);
                PQfinish(conn);

                SPI_connect();
                int lru_idx = lru_find(table_name);
                if (lru_idx >= 0)
                {
                    lru_touch(lru_idx);
                    elog(LOG, "Smart Router: LRU touched for '%s' after INSERT. [%d/%d]",
                         table_name, lru_count, CACHE_MAX_TABLES);
                }
                else
                {
                    elog(LOG, "Smart Router: '%s' not in LRU. Registering (INSERT warm-up).",
                         table_name);
                    lru_add(table_name);
                }
                SPI_finish();

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
                elog(LOG, "Smart Router: UPDATE targeting '%s'. Writing to remote first...",
                     table_name);

                PGconn *conn = PQconnectdb(
                    "host=127.0.0.1 port=6000 dbname=company_remote user=mradul");

                if (PQstatus(conn) != CONNECTION_OK)
                {
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
                    elog(ERROR, "Smart Router: Remote UPDATE failed for '%s': %s.",
                         table_name, PQresultErrorMessage(res));
                    PQclear(res);
                    PQfinish(conn);
                    in_hook = false;
                    ereport(ERROR,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Smart Router: Remote UPDATE failed. Local cache not modified.")));
                }

                elog(LOG, "Smart Router: Remote UPDATE succeeded for '%s'.", table_name);
                PQclear(res);
                PQfinish(conn);

                SPI_connect();
                int lru_idx = lru_find(table_name);
                if (lru_idx >= 0)
                {
                    lru_touch(lru_idx);
                    elog(LOG, "Smart Router: LRU touched for '%s' after UPDATE. [%d/%d]",
                         table_name, lru_count, CACHE_MAX_TABLES);
                }
                else
                {
                    elog(LOG, "Smart Router: '%s' not in LRU. Local UPDATE is a no-op.",
                         table_name);
                }
                SPI_finish();

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
                elog(LOG, "Smart Router: DELETE targeting '%s'. Writing to remote first...",
                     table_name);

                PGconn *conn = PQconnectdb(
                    "host=127.0.0.1 port=6000 dbname=company_remote user=mradul");

                if (PQstatus(conn) != CONNECTION_OK)
                {
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
                    elog(ERROR, "Smart Router: Remote DELETE failed for '%s': %s.",
                         table_name, PQresultErrorMessage(res));
                    PQclear(res);
                    PQfinish(conn);
                    in_hook = false;
                    ereport(ERROR,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Smart Router: Remote DELETE failed. Local cache not modified.")));
                }

                elog(LOG, "Smart Router: Remote DELETE succeeded for '%s'.", table_name);
                PQclear(res);
                PQfinish(conn);

                SPI_connect();
                int lru_idx = lru_find(table_name);
                bool is_full_delete = (strcasestr(query_text, "WHERE") == NULL);

                if (lru_idx >= 0)
                {
                    if (is_full_delete)
                    {
                        elog(LOG,
                             "Smart Router: Full DELETE on '%s'. Evicting from LRU. [%d/%d]",
                             table_name, lru_count - 1, CACHE_MAX_TABLES);
                        lru_cache[lru_idx] = lru_cache[lru_count - 1];
                        lru_count--;
                    }
                    else
                    {
                        lru_touch(lru_idx);
                        elog(LOG,
                             "Smart Router: Partial DELETE on '%s'. LRU touched. [%d/%d]",
                             table_name, lru_count, CACHE_MAX_TABLES);
                    }
                }
                else
                {
                    elog(LOG, "Smart Router: '%s' not in LRU. Local DELETE is a no-op.",
                         table_name);
                }
                SPI_finish();

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
    prev_ExecutorStart = ExecutorStart_hook;
    ExecutorStart_hook = smart_router_ExecutorStart;
}

void smart_router_main(Datum main_arg)
{
    pqsignal(SIGTERM, die);
    BackgroundWorkerUnblockSignals();

    BackgroundWorkerInitializeConnection("postgres", NULL, 0);
    elog(LOG, "Smart Router Schema Synchronizer started successfully inside contrib!");

    PGconn *conn;
    const char *conninfo = "host=127.0.0.1 port=6000 dbname=company_remote user=mradul";
    elog(LOG, "Smart Router: Attempting to connect to remote master at port 6000...");

    conn = PQconnectdb(conninfo);

    if (PQstatus(conn) != CONNECTION_OK)
    {
        elog(ERROR, "Smart Router: Connection to remote database failed: %s",
             PQerrorMessage(conn));
        PQfinish(conn);
        proc_exit(1);
    }
    elog(LOG, "Smart Router: Successfully connected to remote master! Fetching schema...");

    const char *query =
        "SELECT table_name, column_name, data_type "
        "FROM information_schema.columns "
        "WHERE table_schema='public' "
        "ORDER BY table_name, ordinal_position";
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
        SetCurrentStatementStartTimestamp();
        StartTransactionCommand();
        SPI_connect();
        PushActiveSnapshot(GetTransactionSnapshot());

        char current_table[256] = "";
        StringInfoData create_sql;
        initStringInfo(&create_sql);

        for (int i = 0; i < rows; i++)
        {
            char *table_name  = PQgetvalue(res, i, 0);
            char *column_name = PQgetvalue(res, i, 1);
            char *data_type   = PQgetvalue(res, i, 2);

            if (strcmp(current_table, table_name) != 0)
            {
                if (current_table[0] != '\0')
                {
                    appendStringInfoString(&create_sql, ");");
                    elog(LOG, "Smart Router: Executing %s", create_sql.data);
                    SPI_execute(create_sql.data, false, 0);
                }
                strcpy(current_table, table_name);
                resetStringInfo(&create_sql);
                appendStringInfo(&create_sql,
                    "CREATE TABLE IF NOT EXISTS %s (", table_name);
                appendStringInfo(&create_sql, "%s %s", column_name, data_type);
            }
            else
            {
                appendStringInfo(&create_sql, ", %s %s", column_name, data_type);
            }
        }

        if (current_table[0] != '\0')
        {
            appendStringInfoString(&create_sql, ");");
            elog(LOG, "Smart Router: Executing %s", create_sql.data);
            SPI_execute(create_sql.data, false, 0);
        }

        PopActiveSnapshot();
        SPI_finish();
        CommitTransactionCommand();
    }

    PQclear(res);
    PQfinish(conn);
    elog(LOG, "Smart Router Schema Synchronizer finished its initial sync.");
    proc_exit(0);
}