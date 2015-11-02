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

#include <inttypes.h>

#include "sep/am_record.h"

/*
 * This class implements the logic of a predicate. A predicate has the form:
 * <x> op <y>, where x is an AM attribute, op an operator belonging to
 * (>,<,>=,<=) and y a constant.
 *
 * Predicate class consists of:
 * 1) the offset we need to move within the AM record in order to retrieve the
 *    value of the attribute being involved in the predicate.
 * 2) a constant
 * 3) an evaluation function (>,<,>=,<=). For the evaluation function we use a
 *    function pointer being registered upon predicate creation. This way each
 *    predicate knows the way it should be evaluated and we do not need to switch
 *    between the different alternatives. By doing so, we speed up the evaluation
 *    procedure.
 *
 * We have chosen void* as the type of constant with the aim to be able to
 * maintain an array of Predicates. If we had templated the Predicate class
 * (template parameter for the constant type) this would not have been
 * possible.
 *
 * Sample Usage: Predicate p();
 *               p.setOffset(8); //first attribute after timestamp
 *               EvalFPtr fun = gr<int>;
 *               p.setEvalFun(fun);
 *               int constant = 5;
 *               p.setConstant<int>(constant);
 *               AMRecord record(arguments);
 *               bool result = p.evaluate(record);
 */
class Predicate
{
public:
    class Data {
        void *data;
    };
    typedef bool (*EvalFPtr)(const AMRecord &record, uint16_t offset,
                             Data constant);
    Predicate() = default;

public:
    bool evaluate(const AMRecord &record) const;
    void setOffset(uint16_t offset);

    template <typename T>
    void setConstant(T constant);
    void setEvalFun(EvalFPtr fun);

private:
    uint16_t _offset = 0;
    Data _constant;
    EvalFPtr _eval_fun = nullptr;
};

/*
 * Acts as a wrapper for the function pointer _eval_fun.
 */
inline bool
Predicate::evaluate(const AMRecord &record) const
{
    return _eval_fun(record, _offset, _constant);
}

/*
 * This function is templated in order to support all the alternative data
 * types for constant.
 */
template <typename T>
void
Predicate::setConstant(T constant)
{
    static_assert(sizeof(T) <= sizeof(_constant), "type too large");
    memcpy(&_constant, &constant, sizeof(T));
}

/*
 * This function results in the value of the AM attribute this predicate is
 * interested in. It returns the value in the correct data type. The template
 * parameter shows us how much bytes we should read and what is the right cast
 * that must be applied.
 */
template <typename T>
T getValue(const AMRecord &record, uint16_t offset)
{
    T value;
    memcpy(&value, &record.data()[offset], sizeof(T));
    return value;
}

/*
 * The functions implementing the various supported operators follow. All of
 * them are templated in order to support the different data types (int, double,
 * ...). We use the type T for casting the constant, as well as the AM attribute
 * value in the right data type.
 */
template <typename T>
bool gr(const AMRecord &record, uint16_t offset, Predicate::Data constant)
{
    T value = getValue<T>(record, offset);
    T t_constant = *reinterpret_cast<T*>(&constant);
    return (value > t_constant);
}

template <typename T>
bool gre(const AMRecord &record, uint16_t offset, Predicate::Data constant)
{
    T value = getValue<T>(record, offset);
    T t_constant = *reinterpret_cast<T*>(&constant);
    return (value >= t_constant);
}

template <typename T>
bool lt(const AMRecord &record, uint16_t offset, Predicate::Data constant)
{
    T value = getValue<T>(record, offset);
    T t_constant = *reinterpret_cast<T*>(&constant);
    return (value < t_constant);
}

template <typename T>
bool lte(const AMRecord &record, uint16_t offset, Predicate::Data constant)
{
    T value = getValue<T>(record, offset);
    T t_constant = *reinterpret_cast<T*>(&constant);
    return (value <= t_constant);
}

template <typename T>
bool eq(const AMRecord &record, uint16_t offset, Predicate::Data constant)
{
    T value = getValue<T>(record, offset);
    T t_constant = *reinterpret_cast<T*>(&constant);
    return (value == t_constant);
}
