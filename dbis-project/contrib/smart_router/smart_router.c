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

static void smart_router_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
    if (!in_hook && queryDesc->sourceText && strncmp(queryDesc->sourceText, "SELECT", 6) == 0)
    {
        in_hook = true;
        // Very basic prototype caching logic
        elog(LOG, "Smart Router Hook Intercepted Query: %s", queryDesc->sourceText);
        
        char table_name[64] = {0};
        const char *from_ptr = strstr(queryDesc->sourceText, "FROM ");
        if (!from_ptr) from_ptr = strstr(queryDesc->sourceText, "from ");
        if (!from_ptr) from_ptr = strstr(queryDesc->sourceText, "From ");
        
        if (from_ptr) {
            from_ptr += 5; // Skip "FROM "
            // Skip any spaces
            while (*from_ptr == ' ' || *from_ptr == '\t' || *from_ptr == '\n') from_ptr++;
            
            int i = 0;
            // Extract the table name until space, semicolon, newline, or max length
            while (from_ptr[i] != '\0' && from_ptr[i] != ' ' && from_ptr[i] != ';' && from_ptr[i] != '\n' && from_ptr[i] != '\t' && i < 63) {
                // Convert to lowercase to handle case-insensitivity of PG table names (assuming unquoted)
                char c = from_ptr[i];
                if (c >= 'A' && c <= 'Z') c = c + ('a' - 'A');
                table_name[i] = c;
                i++;
            }
            table_name[i] = '\0';
        }

        if (table_name[0] != '\0')
        {
            elog(LOG, "Smart Router: Checking local cache for table %s...", table_name);
            SPI_connect();
            
            char check_q[256];
            snprintf(check_q, sizeof(check_q), "SELECT count(*) FROM %s", table_name);
            int ret = SPI_execute(check_q, true, 0);
            
            bool is_empty = true;
            if (ret == SPI_OK_SELECT && SPI_processed > 0 && SPI_tuptable != NULL)
            {
                char *val = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
                if (val && strcmp(val, "0") != 0) {
                    is_empty = false;
                }
            }
            
            if (is_empty)
            {
                elog(LOG, "Smart Router: Cache miss for %s! Fetching from remote...", table_name);
                
                PGconn *conn = PQconnectdb("host=127.0.0.1 port=6000 dbname=company_remote user=mradul");
                if (PQstatus(conn) == CONNECTION_OK)
                {
                    PGresult *res = PQexec(conn, queryDesc->sourceText);
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
                                // Very basic escaping for prototype
                                if (j > 0) appendStringInfoString(&insert_sql, ", ");
                                appendStringInfo(&insert_sql, "'%s'", val); // Note: proper quoting needed for real production
                            }
                            appendStringInfoString(&insert_sql, ")");
                            
                            SPI_execute(insert_sql.data, false, 0);
                        }
                        elog(LOG, "Smart Router: Cached %d rows for %s.", rows, table_name);
                    }
                    PQclear(res);
                }
                PQfinish(conn);
            }
            else
            {
                elog(LOG, "Smart Router: Cache hit for %s!", table_name);
            }
            SPI_finish();
        }
        in_hook = false;
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