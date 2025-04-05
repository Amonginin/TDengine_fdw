// 测试：执行SQL-建库和表

// TAOS standard API example. The same syntax as MySQL, but only a subset
// to compile: gcc -o create_db_demo create_db_demo.c -ltaos

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taosws.h"

static int DemoCreateDB() {
  ws_enable_log("debug");
  // ANCHOR: create_db_and_table
  int   code = 0;
  char *dsn = "ws://localhost:6041";
  // connect
  WS_TAOS *taos = ws_connect(dsn);

  if (taos == NULL) {
    fprintf(stderr, "Failed to connect to %s, ErrCode: 0x%x, ErrMessage: %s.\n", dsn, ws_errno(NULL), ws_errstr(NULL));
    return -1;
  }

  // create database
  WS_RES *result = ws_query(taos, "CREATE DATABASE IF NOT EXISTS power");
  code = ws_errno(result);
  if (code != 0) {
    fprintf(stderr, "Failed to create database power, ErrCode: 0x%x, ErrMessage: %s.\n", code, ws_errstr(result));
    ws_close(taos);
    return -1;
  }
  ws_free_result(result);
  fprintf(stdout, "Create database power successfully.\n");

  // create table
  const char *sql =
      "CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId "
      "INT, location BINARY(24))";
  result = ws_query(taos, sql);
  code = ws_errno(result);
  if (code != 0) {
    fprintf(stderr, "Failed to create stable power.meters, ErrCode: 0x%x, ErrMessage: %s\n.", code, ws_errstr(result));
    ws_close(taos);
    return -1;
  }
  ws_free_result(result);
  fprintf(stdout, "Create stable power.meters successfully.\n");

  // close & clean
  ws_close(taos);
  return 0;
  // ANCHOR_END: create_db_and_table
}

int main(int argc, char *argv[]) 
{ 
    return DemoCreateDB(); 
}