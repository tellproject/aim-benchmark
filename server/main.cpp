/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Markus Pilman <mpilman@inf.ethz.ch>
 *     Simon Loesing <sloesing@inf.ethz.ch>
 *     Thomas Etter <etterth@gmail.com>
 *     Kevin Bocksrocker <kevin.bocksrocker@gmail.com>
 *     Lucas Braun <braunl@inf.ethz.ch>
 */
#include <iostream>

#include "rta/communication/InfinibandRTACommunication.h"
#include "rta/communication/TCPRTACommunication.h"
#include "rta/dimension_schema.h"
#include "sep/communication/InfinibandSEPCommunication.h"
#include "sep/communication/TCPSEPCommunication.h"
#include "sep/schema_and_index_builder.h"
#include "sep/sep.h"
#include "sep_config.h"

using namespace std;

/*
 * It prints all the configuration details for a run.
 */
void printInfo(uint server_id, string sep_protocol, string rta_protocol,
               const AIMSchema &schema, const CampaignIndex &campaign_index)
{
    cout << "Server id: " << server_id << endl;
    cout << "Server number: " << SERVER_NUM << endl;
    cout << "SEP Protocol: " << sep_protocol << endl;
    cout << "RTA Protocol: " << rta_protocol << endl;
    cout << "Records/bucket: " << RECORDS_PER_BUCKET << endl;
    cout << "SEP Threads: " << SEP_THREAD_NUM << endl;
    cout << "RTA Threads: " << RTA_THREAD_NUM << endl;
    cout << "Total Subscribers: " << SUBSCR_NUM << endl;
    cout << "Subscribers on this Server: " << SUBSCR_NUM / SERVER_NUM << endl;
    cout << "Number of AM attrs: " << schema.numOfEntries() << endl;
    cout << "Record size (bytes): " << schema.size() << endl;
    cout << "Number of active campaigns: " << campaign_index.campaignNum() << endl;
    cout << "Number of entry indexes: " << campaign_index.indexedNum() << endl;
    cout << "Number of unindexed conjuncts: " << campaign_index.unindexedNum() << endl;
}

int main(int argc, const char* argv[])
{
    if (argc != 4 ) {
        std::cout << "usage: ./aim <Server Id><SEP protocol><RTA protocol>"
                  << std::endl;
        return 1;
    }
    try {
        SchemaAndIndexBuilder builder(SQLITE_DB);
        AIMSchema aim_schema = builder.buildAIMSchema();
        DimensionSchema dim_schema;

#ifndef NDEBUG
        cout << "DEBUG\n" << aim_schema << endl;
#endif
        CampaignIndex campaign_index = builder.buildCampaignIndex(aim_schema);
        std::string sep_protocol(argv[2]);
        std::string rta_protocol(argv[3]);
        printInfo(stoi(argv[1]), sep_protocol, rta_protocol, aim_schema, campaign_index);

        unique_ptr<AbstractSEPCommunication> sep_com;
        unique_ptr<AbstractRTACommunication> rta_com;
        if (sep_protocol.compare("Inf") == 0) {
            sep_com.reset(new InfinibandSEPCommunication(SEP_PORT));
        }
        else {
            sep_com.reset(new TCPSEPCommunication(SEP_PORT));
        }
        if (rta_protocol.compare("Inf") == 0) {
            rta_com.reset(new InfinibandRTACommunication(RTA_PORT, RTA_THREAD_NUM, 1, aim_schema, dim_schema));
        }
        else {
            rta_com.reset(new TCPRTACommunication(RTA_PORT, RTA_THREAD_NUM, 1, aim_schema, dim_schema));
        }
        Sep sep(stoi(argv[1]), aim_schema, dim_schema, campaign_index,
                move(rta_com), move(sep_com));
        sep.execute();
    }
    catch (std::exception& exeption) {
        cerr << exeption.what() << endl;
        return 3;
    }
    return 0;
}
