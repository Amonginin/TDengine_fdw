
#include <sstream>


extern "C"
{
#include "query_cxx.h"
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