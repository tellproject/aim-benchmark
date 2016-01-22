#pragma once

#include <tellstore/ScanMemory.hpp>

namespace aim {

struct Context {
    bool isInitialized = false;

    tell::store::ScanMemoryManager *scanMemoryMananger;

    std::vector<std::pair<id_t, AIMSchemaEntry>> tellIDToAIMSchemaEntry;

    id_t subscriberId;
    id_t timeStampId;

    id_t callsSumLocalWeek;
    id_t callsSumAllWeek;
    id_t callsSumAllDay;

    id_t durSumAllWeek;
    id_t durSumAllDay;
    id_t durSumLocalWeek;

    id_t durMaxLocalWeek;
    id_t durMaxLocalDay;
    id_t durMaxDistantWeek;
    id_t durMaxDistantDay;

    id_t costMaxAllWeek;
    id_t costSumAllWeek;
    id_t costSumAllDay;
    id_t costSumLocalWeek;
    id_t costSumDistantWeek;

    id_t subscriptionTypeId;

    id_t regionZip;
    id_t regionCity;
    id_t regionCountry;
    id_t regionRegion;

    id_t valueTypeId;

};

} // namespace aim

