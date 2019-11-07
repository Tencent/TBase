#include "gtm/gtm_c.h"
#include "gtm/gtm_gxid.h"

/*
 * GlobalTransactionIdPrecedes --- is id1 logically < id2?
 */
bool
GlobalTransactionIdPrecedes(GlobalTransactionId id1, GlobalTransactionId id2)
{
    /*
     * If either ID is a permanent XID then we can just do unsigned
     * comparison.    If both are normal, do a modulo-2^31 comparison.
     */
    int32        diff;

    if (!GlobalTransactionIdIsNormal(id1) || !GlobalTransactionIdIsNormal(id2))
        return (id1 < id2);

    diff = (int32) (id1 - id2);
    return (diff < 0);
}

/*
 * GlobalTransactionIdPrecedesOrEquals --- is id1 logically <= id2?
 */
bool
GlobalTransactionIdPrecedesOrEquals(GlobalTransactionId id1, GlobalTransactionId id2)
{
    int32        diff;

    if (!GlobalTransactionIdIsNormal(id1) || !GlobalTransactionIdIsNormal(id2))
        return (id1 <= id2);

    diff = (int32) (id1 - id2);
    return (diff <= 0);
}

/*
 * GlobalTransactionIdFollows --- is id1 logically > id2?
 */
bool
GlobalTransactionIdFollows(GlobalTransactionId id1, GlobalTransactionId id2)
{
    int32        diff;

    if (!GlobalTransactionIdIsNormal(id1) || !GlobalTransactionIdIsNormal(id2))
        return (id1 > id2);

    diff = (int32) (id1 - id2);
    return (diff > 0);
}

/*
 * GlobalTransactionIdFollowsOrEquals --- is id1 logically >= id2?
 */
bool
GlobalTransactionIdFollowsOrEquals(GlobalTransactionId id1, GlobalTransactionId id2)
{
    int32        diff;

    if (!GlobalTransactionIdIsNormal(id1) || !GlobalTransactionIdIsNormal(id2))
        return (id1 >= id2);

    diff = (int32) (id1 - id2);
    return (diff >= 0);
}

