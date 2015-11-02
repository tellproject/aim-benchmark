#pragma once

#include <string>
#include <thread>

/*
 * This file contains a bunch of constants and auxiliary information we use
 * for the project.
 */
typedef unsigned long long Timestamp;
typedef unsigned long ulong;
typedef unsigned int uint;

/*
 * Different types of predicate operators.
 */
enum class Operator {LTE=0, LT, E, GRE, GR, LIKE};

/*
 * C++ data type of AM attributes.
 */
enum class DataType {INT=0, UINT, ULONG, DOUBLE};

/*
 * Aggregation types for the MV attributes.
 */
enum class AggrFun {MIN=0, MAX, SUM};

/*
 * Metrics we have considered.
 */
enum class Metric {CALL=0, DUR, COST};

/*
 * The different window types.
 */
enum class WindowType {TUMB=0, STEP, CONT};

enum class WindowLength {DAY=0, WEEK};

/*
 * The different filter types.
 */
enum class FilterType {NO=0, LOCAL, NONLOCAL};

/*
 * Different options for the firing policy of a campaign.
 */
enum class FiringInterval {ALWAYS=0, ONEDAY, TWODAYS, ONEWEEK};
enum class FiringStartCond {FIXED=0, SLIDING};

/*
 * Number of milliseconds per day.
 */
const ulong MSECS_PER_DAY = 86400000;

/*
 * Number of milliseconds per week.
 */
const ulong MSECS_PER_WEEK = 604800000;

/*
 * We chose the timestamp of the 1st Monday of 1970 as the default starting
 * point for our AM attributes.
 */
const Timestamp FIRST_MONDAY= 345600000;

std::string stringDataType(DataType type);
std::string stringAggrFun(AggrFun fun);
std::string stringFilter(FilterType type);
std::string stringMetric(Metric metric);
std::string stringWindowType(WindowType type);
std::string stringWindowSize(ulong size);

/*
 * This formula determines whether a subscriber should be stored
 * in the underline server. Same formula is used on the sep client
 * side.
 */
uint32_t belongToThisServer(ulong sub_id, uint server_id);

/*
 * Returns the number of cores the system has.
 */
uint findAvailableCores();
