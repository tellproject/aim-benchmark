#include "schema_and_index_builder.h"

SchemaAndIndexBuilder::SchemaAndIndexBuilder(const char *db)
{
    int rc = sqlite3_open_v2(db, &_conn, SQLITE_OPEN_READONLY, NULL);
    if (rc != SQLITE_OK) {
        std::cerr << "DB connection failed: " << rc << std::endl;
        assert(false);
        exit(-1);
    }
}

SchemaAndIndexBuilder::~SchemaAndIndexBuilder()
{
    int rc = sqlite3_close_v2(_conn);
    assert(rc == SQLITE_OK);
}

AIMSchema SchemaAndIndexBuilder::buildAIMSchema()
{
    return _aim_schema_builder.build(_conn);
}

CampaignIndex
SchemaAndIndexBuilder::buildCampaignIndex(const AIMSchema &schema)
{
    return _campaign_index_builder.build(_conn, schema);
}
