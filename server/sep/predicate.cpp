#include "predicate.h"

void
Predicate::setOffset(uint16_t offset)
{
    _offset = offset;
}

void Predicate::setEvalFun(EvalFPtr fun)
{
    _eval_fun = fun;
}
