
using System.Text;

namespace Minerva.DB_Server.Consts;

public static class Network
{
    public const int RAFT_PORT_OFFSET = 1;
    public const int SERVER_MSG_PORT_OFFSET = 2;
    
}


public static class Storage
{
    public const int MAX_YCSB_SHARDS = 100;
    public const int INIT_KV_TABLE_CAP = 200000;
    
    // TPC-C constants
    public const int MAX_TPCC_WAREHOUSE = 1000;
    public const int DISTRICTS_PER_WAREHOUSE = 10;
    public const int CUSTOMERS_PER_DISTRICT = 3000;
    public const int MAX_ITEMS = 100000;
}