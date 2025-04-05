// 测试：执行SQL-查询数据

// TAOS standard API example. The same syntax as MySQL, but only a subset
// to compile: gcc -o query_data_demo query_data_demo.c -ltaos

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taosws.h"

static int DemoQueryData() {
  // ANCHOR: query_data
  int   code = 0;
  char *dsn = "ws://localhost:6041";

  // connect
  WS_TAOS *taos = ws_connect(dsn);
  if (taos == NULL) {
    fprintf(stderr, "Failed to connect to %s, ErrCode: 0x%x, ErrMessage: %s.\n", dsn, ws_errno(NULL), ws_errstr(NULL));
    return -1;
  }

  // query data, please make sure the database and table are already created
  const char *sql = "SELECT ts, current, location FROM power.meters limit 100";
  WS_RES     *result = ws_query(taos, sql);
  code = ws_errno(result);
  if (code != 0) {
    fprintf(stderr, "Failed to query data from power.meters, sql: %s, ErrCode: 0x%x, ErrMessage: %s\n.", sql, code,
            ws_errstr(result));
    ws_close(taos);
    return -1;
  }

  WS_ROW          row = NULL;
  int             rows = 0;
  int             num_fields = ws_field_count(result);
  const WS_FIELD *fields = ws_fetch_fields(result);

  fprintf(stdout, "query successfully, got %d fields, the sql is: %s.\n", num_fields, sql);

  // fetch the records row by row
  while ((row = ws_fetch_row(result))) {
    // Add your data processing logic here

    rows++;
  }
  fprintf(stdout, "total rows: %d\n", rows);
  ws_free_result(result);

  // close & clean
  ws_close(taos);
  return 0;
  // ANCHOR_END: query_data
}

int main(int argc, char *argv[]) { return DemoQueryData(); }