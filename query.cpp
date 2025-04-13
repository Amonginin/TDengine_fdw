
#include <sstream>

#include "connection.hpp"
#include <taosws.h>
extern "C"
{
#include "query_cxx.h"
}

/*
 * bindParameter
 *      bind parameter to prepare for query exection
 */
static influxdb::InfluxDBParams
bindParameter(InfluxDBType *param_type, InfluxDBValue *param_val, int param_num)
{
    influxdb::InfluxDBParams params;

    if (param_num > 0)
    {
        for (int i = 0; i < param_num; i++)
        {
            /* Each placeholder is "$1", "$2",...,so set "1","2",... to map key */
            switch (param_type[i])
            {
                case INFLUXDB_STRING:
                    params.addParam(std::to_string(i + 1), std::string(param_val[i].s));
                    break;
                case INFLUXDB_INT64:
                case INFLUXDB_TIME:
                    params.addParam(std::to_string(i + 1), param_val[i].i);
                    break;
                case INFLUXDB_BOOLEAN:
                    params.addParam(std::to_string(i + 1), (bool) param_val[i].b);
                    break;
                case INFLUXDB_DOUBLE:
                    params.addParam(std::to_string(i + 1), param_val[i].d);
                    break;
                case INFLUXDB_NULL:
                    params.addParam(std::to_string(i + 1), "\"\"");
                    break;
                default:
                    elog(ERROR, "Unexpected type: %d", param_type[i]);
            }
        }
    }

    return params;
}
/*
 * TDengineQuery
 *      execute single InfluxQL query
 */
extern "C" struct TDengineQuery_return
TDengineQuery(char* cquery, UserMapping *user, tdengine_opt *opts, TDengineType* ctypes, TDengineValue* cvalues, int cparamNum)
{
    TDengineQuery_return *res = (TDengineQuery_return *) palloc0(sizeof(TDengineQuery_return));
    auto influx = tdengine_get_connection(user, opts);
    auto params = bindParameter(ctypes, cvalues, cparamNum);

    try
    {
        auto result_set = influx->query(std::string(cquery), params);

        /* Use first statement result */
        if (result_set.size() > 0)
        {
            auto query_result = result_set.at(0);
            if (query_result.error.length() > 0)
            {
                res->r1 = (char *) palloc0(sizeof(char) * (query_result.error.length()) + 1);
                strcpy(res->r1, query_result.error.c_str());
            }
            else
                res->r0 = TDengineSeries_to_TDengineResult(query_result.series);
        }
    }
    catch (const std::exception& e)
    {
        res->r1 = (char *) palloc0(sizeof(char) * (strlen(e.what()) + 1));
        strcpy(res->r1, e.what());
    }

    return *res;
}