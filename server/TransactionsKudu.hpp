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
#pragma once

#include <vector>
#include <string>

#include <common/Protocol.hpp>
#include <common/Util.hpp>
#include <kudu/client/client.h>

#include "server/sep/aim_schema.h"

namespace aim {

    // general field names used in the AIM benchmark
    static const std::string subscriberId = "subscriber_id";
    static const std::string timeStamp = "last_updated";

    static const std::string subscriptionTypeId = "subscription_type_id";

    static const std::string regionZip = "city_zip";
    static const std::string regionCity = "region_cty_id";
    static const std::string regionCountry = "region_country_id";
    static const std::string regionRegion = "region_region_id";

    static const std::string valueTypeId = "value_type_id";

    static const std::string categoryId = "category_id";

    // general field indexes used in the AIM benchmark
    static const int sSsubscriberIdIdx = 0;
    static const int sTimeStampIdx = 1;

static std::vector<std::string> getRecordProjection(const AIMSchema &aimSchema) {
    std::vector<std::string> result;
    result.emplace_back(subscriberId);
    result.emplace_back(timeStamp);
    for (uint i = 0; i < aimSchema.numOfEntries(); ++i) {
        result.emplace_back(aimSchema[i].name().c_str(), aimSchema[i].name().size());
    }
    return result;
}

class Transactions {


public:

    /**
     * takes a transaction in the constructor such that schema can be obained at startup time
     */
    Transactions(const AIMSchema &aimSchema):
            mAimSchema(aimSchema),
            mEventProjection(getRecordProjection(aimSchema))
    {
        auto crossbow_string = mAimSchema.getName(Metric::CALL, AggrFun::SUM, FilterType::LOCAL, WindowLength::WEEK);
        callsSumLocalWeek = std::string (crossbow_string.c_str(), crossbow_string.size());
        crossbow_string = mAimSchema.getName(Metric::CALL, AggrFun::SUM, FilterType::NO, WindowLength::WEEK);
        callsSumAllWeek = std::string (crossbow_string.c_str(), crossbow_string.size());
        crossbow_string = mAimSchema.getName(Metric::CALL, AggrFun::SUM, FilterType::NO, WindowLength::DAY);
        callsSumAllDay = std::string (crossbow_string.c_str(), crossbow_string.size());

        crossbow_string = mAimSchema.getName(Metric::DUR, AggrFun::SUM, FilterType::LOCAL, WindowLength::WEEK);
        durSumLocalWeek = std::string (crossbow_string.c_str(), crossbow_string.size());
        crossbow_string = mAimSchema.getName(Metric::DUR, AggrFun::SUM, FilterType::NO, WindowLength::WEEK);
        durSumAllWeek = std::string (crossbow_string.c_str(), crossbow_string.size());
        crossbow_string = mAimSchema.getName(Metric::DUR, AggrFun::SUM, FilterType::NO, WindowLength::DAY);
        durSumAllDay = std::string (crossbow_string.c_str(), crossbow_string.size());

        crossbow_string = mAimSchema.getName(Metric::DUR, AggrFun::MAX, FilterType::LOCAL, WindowLength::WEEK);
        durMaxLocalWeek = std::string (crossbow_string.c_str(), crossbow_string.size());
        crossbow_string = mAimSchema.getName(Metric::DUR, AggrFun::MAX, FilterType::LOCAL, WindowLength::DAY);
        durMaxLocalDay = std::string (crossbow_string.c_str(), crossbow_string.size());
        crossbow_string = mAimSchema.getName(Metric::DUR, AggrFun::MAX, FilterType::NONLOCAL, WindowLength::WEEK);
        durMaxDistantWeek = std::string (crossbow_string.c_str(), crossbow_string.size());
        crossbow_string = mAimSchema.getName(Metric::DUR, AggrFun::MAX, FilterType::NONLOCAL, WindowLength::DAY);
        durMaxDistantDay = std::string (crossbow_string.c_str(), crossbow_string.size());

        crossbow_string = mAimSchema.getName(Metric::COST, AggrFun::MAX, FilterType::NO, WindowLength::WEEK);
        costMaxAllWeek = std::string (crossbow_string.c_str(), crossbow_string.size());
        crossbow_string = mAimSchema.getName(Metric::COST, AggrFun::SUM, FilterType::NO, WindowLength::WEEK);
        costSumAllWeek = std::string (crossbow_string.c_str(), crossbow_string.size());
        crossbow_string = mAimSchema.getName(Metric::COST, AggrFun::SUM, FilterType::NO, WindowLength::DAY);
        costSumAllDay = std::string (crossbow_string.c_str(), crossbow_string.size());
        crossbow_string = mAimSchema.getName(Metric::COST, AggrFun::SUM, FilterType::LOCAL, WindowLength::WEEK);
        costSumLocalWeek = std::string (crossbow_string.c_str(), crossbow_string.size());
        crossbow_string = mAimSchema.getName(Metric::COST, AggrFun::SUM, FilterType::NONLOCAL, WindowLength::WEEK);
        costSumDistantWeek = std::string (crossbow_string.c_str(), crossbow_string.size());
    }

    void processEvent(kudu::client::KuduSession& session, std::vector<Event> &events);

    Q1Out q1Transaction(kudu::client::KuduSession& session, const Q1In& in);
    Q2Out q2Transaction(kudu::client::KuduSession& session, const Q2In& in);
    Q3Out q3Transaction(kudu::client::KuduSession& session);
    Q4Out q4Transaction(kudu::client::KuduSession& session, const Q4In& in);
    Q5Out q5Transaction(kudu::client::KuduSession& session, const Q5In& in);
    Q6Out q6Transaction(kudu::client::KuduSession& session, const Q6In& in);
    Q7Out q7Transaction(kudu::client::KuduSession& session, const Q7In& in);

private:
    const AIMSchema &mAimSchema;

    // projection for getting all non-static fields (AM attributes)
    const std::vector<std::string> mEventProjection;

    std::string callsSumLocalWeek;
    std::string callsSumAllWeek;
    std::string callsSumAllDay;

    std::string durSumLocalWeek;
    std::string durSumAllWeek;
    std::string durSumAllDay;

    std::string durMaxLocalWeek;
    std::string durMaxLocalDay;
    std::string durMaxDistantWeek;
    std::string durMaxDistantDay;

    std::string costMaxAllWeek;
    std::string costSumAllWeek;
    std::string costSumAllDay;
    std::string costSumLocalWeek;
    std::string costSumDistantWeek;
};

} // namespace aim
