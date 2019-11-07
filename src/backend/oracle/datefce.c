/*
 * Tencent is pleased to support the open source community by making TBase available.  
 * 
 * Copyright (C) 2019 THL A29 Limited, a Tencent company.  All rights reserved.
 * 
 * TBase is licensed under the BSD 3-Clause License, except for the third-party component listed below. 
 * 
 * A copy of the BSD 3-Clause License is included in this file.
 * 
 * Other dependencies and licenses:
 * 
 * Open Source Software Licensed Under the PostgreSQL License: 
 * --------------------------------------------------------------------
 * 1. Postgres-XL XL9_5_STABLE
 * Portions Copyright (c) 2015-2016, 2ndQuadrant Ltd
 * Portions Copyright (c) 2012-2015, TransLattice, Inc.
 * Portions Copyright (c) 2010-2017, Postgres-XC Development Group
 * Portions Copyright (c) 1996-2015, The PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, The Regents of the University of California
 * 
 * Terms of the PostgreSQL License: 
 * --------------------------------------------------------------------
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written agreement
 * is hereby granted, provided that the above copyright notice and this
 * paragraph and the following two paragraphs appear in all copies.
 * 
 * IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 * DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * 
 * THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
 * ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 * 
 * 
 * Terms of the BSD 3-Clause License:
 * --------------------------------------------------------------------
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 * 
 * 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation 
 * and/or other materials provided with the distribution.
 * 
 * 3. Neither the name of THL A29 Limited nor the names of its contributors may be used to endorse or promote products derived from this software without 
 * specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, 
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS 
 * BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE 
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT 
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH 
 * DAMAGE.
 * 
 */
#include "postgres.h"
#include "miscadmin.h"
#include "access/xact.h"
#include "commands/variable.h"
#include "parser/scansup.h"
#include "mb/pg_wchar.h"
#include "utils/date.h"
#include "utils/builtins.h"
#include "utils/nabstime.h"
#include "utils/numeric.h"
#include "utils/formatting.h"
#include <sys/time.h>

#define ENABLE_INTERNATIONALIZED_WEEKDAY

#ifdef ENABLE_INTERNATIONALIZED_WEEKDAY

typedef struct WeekDays
{
    int            encoding;
    const char *names[7];
} WeekDays;

/*
 * { encoding, { "sun", "mon", "tue", "wed", "thu", "fri", "sat" } },
 */
static const WeekDays WEEKDAYS[] =
{
    /* Japanese, UTF8 */
    { PG_UTF8, { "\346\227\245", "\346\234\210", "\347\201\253", "\346\260\264", "\346\234\250", "\351\207\221", "\345\234\237" } },
    /* Japanese, EUC_JP */
    { PG_EUC_JP, { "\306\374", "\267\356", "\262\320", "\277\345", "\314\332", "\266\342", "\305\332" } },
    /* Japanese, EUC_JIS_2004 (same as EUC_JP) */
    { PG_EUC_JIS_2004, { "\306\374", "\267\356", "\262\320", "\277\345", "\314\332", "\266\342", "\305\332" } },
};

static const WeekDays *mru_weekdays = NULL;

static int
weekday_search(const WeekDays *weekdays, const char *str, int len)
{
    int        i;

    for (i = 0; i < 7; i++)
    {
        int    n = strlen(weekdays->names[i]);
        if (n > len)
            continue;    /* too short */
        if (pg_strncasecmp(weekdays->names[i], str, n) == 0)
            return i;
    }
    return -1;    /* not found */
}

#endif    /* ENABLE_INTERNATIONALIZED_WEEKDAY */

#define CASE_fmt_YYYY    case 0: case 1: case 2: case 3: case 4: case 5: case 6:
#define CASE_fmt_IYYY    case 7: case 8: case 9: case 10:
#define    CASE_fmt_Q    case 11:
#define    CASE_fmt_WW    case 12:
#define CASE_fmt_IW    case 13:
#define    CASE_fmt_W    case 14:
#define CASE_fmt_DAY    case 15: case 16: case 17:
#define CASE_fmt_MON    case 18: case 19: case 20: case 21:
#define CASE_fmt_CC    case 22: case 23:
#define CASE_fmt_DDD    case 24: case 25: case 26:
#define CASE_fmt_HH    case 27: case 28: case 29:
#define CASE_fmt_MI    case 30:

const char *date_fmt[] =
{
    "Y", "Yy", "Yyy", "Yyyy", "Year", "Syyyy", "syear",
    "I", "Iy", "Iyy", "Iyyy",
    "Q", "Ww", "Iw", "W",
    "Day", "Dy", "D",
    "Month", "Mon", "Mm", "Rm",
    "Cc", "Scc",
    "Ddd", "Dd", "J",
    "Hh", "Hh12", "Hh24",
    "Mi",
    NULL
};

const char *orcl_days[] = {"Sunday", "Monday", "Tuesday", "Wednesday",
"Thursday", "Friday", "Saturday", NULL};

#define CHECK_SEQ_SEARCH(_l, _s) \
do { \
    if ((_l) < 0) { \
        ereport(ERROR, \
                (errcode(ERRCODE_INVALID_DATETIME_FORMAT), \
                 errmsg("invalid value for %s", (_s)))); \
    } \
} while (0)

/*
 * Search const value in char array
 *
 */
static int
orcl_seq_search(const char *name, const char *array[], int max)
{
    int        i;

    if (!*name)
        return -1;

    for (i = 0; array[i]; i++)
    {
        if (strlen(array[i]) == max &&
            pg_strncasecmp(name, array[i], max) == 0)
            return i;
    }
    return -1;    /* not found */
}

static int
orcl_seq_prefix_search(const char *name, const char *array[], int max)
{
    int        i;

    if (!*name)
        return -1;

    for (i = 0; array[i]; i++)
    {
        if (pg_strncasecmp(name, array[i], max) == 0)
            return i;
    }
    return -1;    /* not found */
}

/********************************************************************
 *
 * next_day
 *
 * Syntax:
 *
 * date next_day(date value, text weekday)
 *
 * Purpose:
 *
 * Returns the first weekday that is greater than a date value.
 *
 ********************************************************************/
Datum
orcl_next_day(PG_FUNCTION_ARGS)
{// #lizard forgives
    DateADT day = PG_GETARG_DATEADT(0);
    text *day_txt = PG_GETARG_TEXT_PP(1);
    const char *str = NULL;
    int    len = 0;
    int off;
    int d = -1;

    if (!day_txt)
        PG_RETURN_NULL();

    str = VARDATA_ANY(day_txt);
    len = VARSIZE_ANY_EXHDR(day_txt);

#ifdef ENABLE_INTERNATIONALIZED_WEEKDAY
    /* Check mru_weekdays first for performance. */
    if (mru_weekdays)
    {
        if ((d = weekday_search(mru_weekdays, str, len)) >= 0)
            goto found;
        else
            mru_weekdays = NULL;
    }
#endif

    /*
     * Oracle uses only 3 heading characters of the input.
     * Ignore all trailing characters.
     */
    if (len >= 3 && (d = orcl_seq_prefix_search(str, orcl_days, 3)) >= 0)
        goto found;

#ifdef ENABLE_INTERNATIONALIZED_WEEKDAY
    do
    {
        int        i;
        int        encoding = GetDatabaseEncoding();

        for (i = 0; i < lengthof(WEEKDAYS); i++)
        {
            if (encoding == WEEKDAYS[i].encoding)
            {
                if ((d = weekday_search(&WEEKDAYS[i], str, len)) >= 0)
                {
                    mru_weekdays = &WEEKDAYS[i];
                    goto found;
                }
            }
        }
    } while(0);
#endif

    CHECK_SEQ_SEARCH(-1, "DAY/Day/day");

found:
    off = d - j2day(day+POSTGRES_EPOCH_JDATE);

    PG_RETURN_DATEADT((off <= 0) ? day+off+7 : day + off);
}

/* next_day(date, integer) is not documented in Oracle manual, but ... */
Datum
orcl_next_day_by_index(PG_FUNCTION_ARGS)
{
    DateADT day = PG_GETARG_DATEADT(0);
    int        idx = PG_GETARG_INT32(1);
    int        off;

    /*
     * off is 1..7 (Sun..Sat).
     *
     * TODO: It should be affected by NLS_TERRITORY. For example,
     * 1..7 should be interpreted as Mon..Sun in GERMAN.
     */
    CHECK_SEQ_SEARCH((idx < 1 || 7 < idx) ? -1 : 0, "DAY/Day/day");

    /* j2day returns 0..6 as Sun..Sat */
    off = (idx - 1) - j2day(day+POSTGRES_EPOCH_JDATE);

    PG_RETURN_DATEADT((off <= 0) ? (day + off + 7) : (day + off));
}

/********************************************************************
 *
 * last_day
 *
 * Syntax:
 *
 * date last_day(date value)
 *
 * Purpose:
 *
 * Returns last day of the month based on a date value
 *
 ********************************************************************/
Datum
orcl_last_day(PG_FUNCTION_ARGS)
{
    DateADT day = PG_GETARG_DATEADT(0);
    DateADT result;
    int y, m, d;
    j2date(day + POSTGRES_EPOCH_JDATE, &y, &m, &d);
    result = date2j(y, m+1, 1) - POSTGRES_EPOCH_JDATE;

    PG_RETURN_DATEADT(result - 1);
}

static const int month_days[] = {
    31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31
};

static int
days_of_month(int y, int m)
{
    int    days;

    if (m < 0 || 12 < m)
        ereport(ERROR,
                (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                 errmsg("date out of range")));

    days = month_days[m - 1];
    if (m == 2 && (y % 400 == 0 || (y % 4 == 0 && y % 100 != 0)))
        days += 1;    /* February 29 in leap year */
    return days;
}

/********************************************************************
 *
 * months_between
 *
 * Syntax:
 *
 * float8 months_between(date date1, date date2)
 *
 * Purpose:
 *
 * Returns the number of months between date1 and date2. If
 *      a fractional month is calculated, the months_between  function
 *      calculates the fraction based on a 31-day month.
 *
 ********************************************************************/
Datum
orcl_months_between(PG_FUNCTION_ARGS)
{
    DateADT date1 = PG_GETARG_DATEADT(0);
    DateADT date2 = PG_GETARG_DATEADT(1);

    int y1, m1, d1;
    int y2, m2, d2;

    float8 result;

    j2date(date1 + POSTGRES_EPOCH_JDATE, &y1, &m1, &d1);
    j2date(date2 + POSTGRES_EPOCH_JDATE, &y2, &m2, &d2);

    /* Ignore day components for last days, or based on a 31-day month. */
    if (d1 == days_of_month(y1, m1) && d2 == days_of_month(y2, m2))
        result = (y1 - y2) * 12 + (m1 - m2);
    else
        result = (y1 - y2) * 12 + (m1 - m2) + (d1 - d2) / 31.0;

    PG_RETURN_NUMERIC(
        DirectFunctionCall1(float8_numeric, Float8GetDatumFast(result)));
}

/********************************************************************
 *
 * add_months
 *
 * Syntax:
 *
 * date add_months(date day, int val)
 *
 * Purpose:
 *
 * Returns a date plus n months.
 *
 ********************************************************************/
Datum
orcl_add_months(PG_FUNCTION_ARGS)
{
    DateADT day = PG_GETARG_DATEADT(0);
    int n = PG_GETARG_INT32(1);
    int y, m, d;
    int    days;
    DateADT result;
    div_t    v;
    bool    last_day;

    j2date(day + POSTGRES_EPOCH_JDATE, &y, &m, &d);
    last_day = (d == days_of_month(y, m));

    v = div(y * 12 + m - 1 + n, 12);
    y = v.quot;
    if (y < 0)
        y += 1;    /* offset because of year 0 */
    m = v.rem + 1;

    days = days_of_month(y, m);
    if (last_day || d > days)
        d = days;

    result = date2j(y, m, d) - POSTGRES_EPOCH_JDATE;

    PG_RETURN_DATEADT (result);
}

/*
 * ISO year
 *
 */
#define DATE2J(y,m,d)    (date2j((y),(m),(d)) - POSTGRES_EPOCH_JDATE)
#define J2DAY(date)    (j2day(date + POSTGRES_EPOCH_JDATE))

static DateADT
iso_year (int y, int m, int d)
{
    DateADT result, result2, day;
    int off;

    result = DATE2J(y,1,1);
    day = DATE2J(y,m,d);
    off = 4 - J2DAY(result);
    result += off + ((off >= 0) ? - 3: + 4);  /* to monday */

    if (result > day)
    {
        result = DATE2J(y-1,1,1);
        off = 4 - J2DAY(result);
        result += off + ((off >= 0) ? - 3: + 4);  /* to monday */
    }

    if (((day - result) / 7 + 1) > 52)
    {
    result2 = DATE2J(y+1,1,1);
    off = 4 - J2DAY(result2);
    result2 += off + ((off >= 0) ? - 3: + 4);  /* to monday */

    if (day >= result2)
        return result2;
    }

    return result;
}

static DateADT
_orcl_date_trunc(DateADT day, int f)
{
    int y, m, d;
    DateADT result;

    j2date(day + POSTGRES_EPOCH_JDATE, &y, &m, &d);

    switch (f)
    {
        CASE_fmt_CC
            if (y > 0)
                result = DATE2J((y/100)*100+1,1,1);
            else
                result = DATE2J(-((99 - (y - 1)) / 100) * 100 + 1,1,1);
            break;
        CASE_fmt_YYYY
            result = DATE2J(y,1,1);
            break;
        CASE_fmt_IYYY
            result = iso_year(y,m,d);
            break;
        CASE_fmt_MON
            result = DATE2J(y,m,1);
            break;
        CASE_fmt_WW
            result = day - (day - DATE2J(y,1,1)) % 7;
            break;
        CASE_fmt_IW
            result = day - (day - iso_year(y,m,d)) % 7;
            break;
        CASE_fmt_W
            result = day - (day - DATE2J(y,m,1)) % 7;
            break;
        CASE_fmt_DAY
            result = day - J2DAY(day);
            break;
        CASE_fmt_Q
            result = DATE2J(y,((m-1)/3)*3+1,1);
            break;
        default:
            result = day;
    }

    return result;
}

static DateADT
_orcl_date_round(DateADT day, int f)
{// #lizard forgives
    int y, m, d, z;
    DateADT result;

    j2date(day + POSTGRES_EPOCH_JDATE, &y, &m, &d);

    switch (f)
    {
        CASE_fmt_CC
            if (y > 0)
                result = DATE2J((y/100)*100+(day < DATE2J((y/100)*100+50,1,1) ?1:101),1,1);
            else
                result = DATE2J((y/100)*100+(day < DATE2J((y/100)*100-50+1,1,1) ?-99:1),1,1);
            break;
        CASE_fmt_YYYY
            result = DATE2J(y+(day<DATE2J(y,7,1)?0:1),1,1);
            break;
        CASE_fmt_IYYY
        {
            if (day < DATE2J(y,7,1))
            {
                result = iso_year(y, m, d);
            }
            else
            {
                DateADT iy1 = iso_year(y+1, 1, 8);
                result = iy1;

                if (((day - DATE2J(y,1,1)) / 7 + 1) >= 52)
                {
                    bool overl = ((date2j(y+2,1,1)-date2j(y+1,1,1)) == 366);
                    bool isSaturday = (J2DAY(day) == 6);

                    DateADT iy2 = iso_year(y+2, 1, 8);
                    DateADT day1 = DATE2J(y+1,1,1);
                    /* exception saturdays */
                    if (iy1 >= (day1) && day >= day1 - 2 && isSaturday)
                    {
                        result = overl?iy2:iy1;
                    }
                    /* iso year stars in last year and day >= iso year */
                    else if (iy1 <= (day1) && day >= iy1 - 3)
                    {
                        DateADT cmp = iy1 - (iy1 < day1?0:1);
                        int d = J2DAY(day1);
                        /* some exceptions */
                        if ((day >= cmp - 2) && (!(d == 3 && overl)))
                        {
                            /* if year don't starts in thursday */
                            if ((d < 4 && J2DAY(day) != 5 && !isSaturday)
                                ||(d == 2 && isSaturday && overl))
                            {
                                result = iy2;
                            }
                        }
                    }
                }
            }
            break;
        }
        CASE_fmt_MON
            result = DATE2J(y,m+(day<DATE2J(y,m,16)?0:1),1);
            break;
        CASE_fmt_WW
            z = (day - DATE2J(y,1,1)) % 7;
            result = day - z + (z < 4?0:7);
            break;
        CASE_fmt_IW
        {
            z = (day - iso_year(y,m,d)) % 7;
            result = day - z + (z < 4?0:7);
            if (((day - DATE2J(y,1,1)) / 7 + 1) >= 52)
            {
                /* only for last iso week */
                DateADT isoyear = iso_year(y+1, 1, 8);
                if (isoyear > (DATE2J(y+1,1,1)-1))
                    if (day > isoyear - 7)
                    {
                        int d = J2DAY(day);
                        result -= (d == 0 || d > 4?7:0);
                    }
            }
            break;
        }
        CASE_fmt_W
            z = (day - DATE2J(y,m,1)) % 7;
            result = day - z + (z < 4?0:7);
            break;
        CASE_fmt_DAY
            z = J2DAY(day);
            if (y > 0)
                result = day - z + (z < 4?0:7);
            else
                result = day + (5 - (z>0?(z>1?z:z+7):7));
            break;
        CASE_fmt_Q
            result = DATE2J(y,((m-1)/3)*3+(day<(DATE2J(y,((m-1)/3)*3+2,16))?1:4),1);
            break;
        default:
            result = day;
    }
    return result;
}

/********************************************************************
 *
 * orcl_date_trunc|orcl_timestamptz_trunc .. trunc
 *
 * Syntax:
 *
 * date trunc(date date1, text format)
 *
 * Purpose:
 *
 * Returns d with the time portion of the day truncated to the unit
 * specified by the format fmt.
 *
 ********************************************************************/
Datum
orcl_date_trunc(PG_FUNCTION_ARGS)
{
    DateADT day = PG_GETARG_DATEADT(0);
    text *fmt = PG_GETARG_TEXT_PP(1);

    DateADT result;

    int f = orcl_seq_search(VARDATA_ANY(fmt), date_fmt, VARSIZE_ANY_EXHDR(fmt));
    CHECK_SEQ_SEARCH(f, "round/trunc format string");

    result = _orcl_date_trunc(day, f);
    PG_RETURN_DATEADT(result);
}

/*
 * Workaround for access to session_timezone on WIN32,
 * 
 * session timezone isn't accessed directly, but taken by show_timezone,
 * and reparsed. For better performance, the result is cached in fn_extra.
 *
 */
static pg_tz *
get_session_timezone(FunctionCallInfo fcinfo)
{
#if defined(WIN32)
    pg_tz *result = (pg_tz *) fcinfo->flinfo->fn_extra;
    if (result == NULL)
    {
        const char *tzn = show_timezone();
        void *extra;

        if (!check_timezone(((char **) &tzn), &extra, PGC_S_CLIENT))
            elog(ERROR, "cannot to parse timezone \"%s\"", tzn);

        result = *((pg_tz **) extra);
        fcinfo->flinfo->fn_extra = result;

        /*
         * check_timezone allocates small block of pg_tz * size. This block
         * should be released by free(extra), but I cannot release memory
         * allocated by application in library on MS platform. So I have to
         * accept small memory leak - elsewhere exception - broken heap :(
         *
         *
         * cannot be called
            free( extra );
         */
    }
    return result;
#else
    return session_timezone;
#endif
}

/*
 * redotz is used only for timestamp with time zone
 */
static void
tm_trunc(struct pg_tm *tm, text *fmt, bool *redotz)
{
    int f;

    f = orcl_seq_search(VARDATA_ANY(fmt), date_fmt, VARSIZE_ANY_EXHDR(fmt));
    CHECK_SEQ_SEARCH(f, "round/trunc format string");

    tm->tm_sec = 0;

    switch (f)
    {
        CASE_fmt_IYYY
        CASE_fmt_WW
        CASE_fmt_W
        CASE_fmt_IW
        CASE_fmt_DAY
        CASE_fmt_CC
            j2date(_orcl_date_trunc(DATE2J(tm->tm_year, tm->tm_mon, tm->tm_mday), f)
                   + POSTGRES_EPOCH_JDATE,
            &tm->tm_year, &tm->tm_mon, &tm->tm_mday);
            tm->tm_hour = 0;
            tm->tm_min = 0;
            *redotz = true;
            break;
        CASE_fmt_YYYY
            tm->tm_mon = 1;
        CASE_fmt_Q
            tm->tm_mon = (3*((tm->tm_mon - 1)/3)) + 1;
        CASE_fmt_MON
            tm->tm_mday = 1;
        CASE_fmt_DDD
            tm->tm_hour = 0;
            *redotz = true; /* for all cases >= DAY */
        CASE_fmt_HH
            tm->tm_min = 0;
    }
}

Datum
orcl_timestamptz_trunc(PG_FUNCTION_ARGS)
{
    TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(0);
    TimestampTz result;
    text *fmt = PG_GETARG_TEXT_PP(1);
    int tz;
    fsec_t fsec;
    struct pg_tm tt, *tm = &tt;
    const char *tzn;
    bool redotz = false;

    if (TIMESTAMP_NOT_FINITE(timestamp))
        PG_RETURN_TIMESTAMPTZ(timestamp);

    if (timestamp2tm(timestamp, &tz, tm, &fsec, &tzn, NULL) != 0)
        ereport(ERROR,
                    (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                     errmsg("timestamp out of range")));

    tm_trunc(tm, fmt, &redotz);
    fsec = 0;

    if (redotz)
        tz = DetermineTimeZoneOffset(tm, get_session_timezone(fcinfo));

    if (tm2timestamp(tm, fsec    , &tz, &result) != 0)
        ereport(ERROR,
                    (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                     errmsg("timestamp out of range")));

    PG_RETURN_TIMESTAMPTZ(result);
}

/********************************************************************
 *
 * orcl_date_round|orcl_timestamptz_round .. round
 *
 * Syntax:
 *
 * date round(date date1, text format)
 *
 * Purpose:
 *
 * Returns d with the time portion of the day roundeded to the unit
 * specified by the format fmt.
 *
 ********************************************************************/
Datum
orcl_date_round(PG_FUNCTION_ARGS)
{
    DateADT day = PG_GETARG_DATEADT(0);
    text *fmt = PG_GETARG_TEXT_PP(1);

    DateADT result;

    int f = orcl_seq_search(VARDATA_ANY(fmt), date_fmt, VARSIZE_ANY_EXHDR(fmt));
    CHECK_SEQ_SEARCH(f, "round/trunc format string");

    result = _orcl_date_round(day, f);
    PG_RETURN_DATEADT(result);
}

#define NOT_ROUND_MDAY(_p_) \
    do { if (_p_) rounded = false; } while(0)
#define ROUND_MDAY(_tm_) \
    do { if (rounded) _tm_->tm_mday += _tm_->tm_hour >= 12?1:0; } while(0)

static void
tm_round(struct pg_tm *tm, text *fmt, bool *redotz)
{// #lizard forgives
    int     f;
    bool    rounded = true;

    f = orcl_seq_search(VARDATA_ANY(fmt), date_fmt, VARSIZE_ANY_EXHDR(fmt));
    CHECK_SEQ_SEARCH(f, "round/trunc format string");

    /* set rounding rule */
    switch (f)
    {
        CASE_fmt_IYYY
            NOT_ROUND_MDAY(tm->tm_mday < 8 && tm->tm_mon == 1);
            NOT_ROUND_MDAY(tm->tm_mday == 30 && tm->tm_mon == 6);
            if (tm->tm_mday >= 28 && tm->tm_mon == 12 && tm->tm_hour >= 12)
            {
                DateADT isoyear = iso_year(tm->tm_year+1, 1, 8);
                DateADT day0 = DATE2J(tm->tm_year+1,1,1);
                DateADT dayc = DATE2J(tm->tm_year, tm->tm_mon, tm->tm_mday);

                if ((isoyear <= day0) || (day0 <= dayc + 2))
                {
                    rounded = false;
                }
            }
            break;
        CASE_fmt_YYYY
            NOT_ROUND_MDAY(tm->tm_mday == 30 && tm->tm_mon == 6);
            break;
        CASE_fmt_MON
            NOT_ROUND_MDAY(tm->tm_mday == 15);
            break;
        CASE_fmt_Q
            NOT_ROUND_MDAY(tm->tm_mday == 15 && tm->tm_mon == ((tm->tm_mon-1)/3)*3+2);
            break;
        CASE_fmt_WW
        CASE_fmt_IW
            /* last day in year */
            NOT_ROUND_MDAY(DATE2J(tm->tm_year, tm->tm_mon, tm->tm_mday) ==
            (DATE2J(tm->tm_year+1, 1,1) - 1));
            break;
        CASE_fmt_W
            /* last day in month */
            NOT_ROUND_MDAY(DATE2J(tm->tm_year, tm->tm_mon, tm->tm_mday) ==
            (DATE2J(tm->tm_year, tm->tm_mon+1,1) - 1));
            break;
    }

    switch (f)
    {
        /* easier convert to date */
        CASE_fmt_IW
        CASE_fmt_DAY
        CASE_fmt_IYYY
        CASE_fmt_WW
        CASE_fmt_W
        CASE_fmt_CC
        CASE_fmt_MON
        CASE_fmt_YYYY
        CASE_fmt_Q
            ROUND_MDAY(tm);
            j2date(_orcl_date_round(DATE2J(tm->tm_year, tm->tm_mon, tm->tm_mday), f)
                   + POSTGRES_EPOCH_JDATE,
            &tm->tm_year, &tm->tm_mon, &tm->tm_mday);
            tm->tm_hour = 0;
            tm->tm_min = 0;
            *redotz = true;
            break;
        CASE_fmt_DDD
            tm->tm_mday += (tm->tm_hour >= 12)?1:0;
            tm->tm_hour = 0;
            tm->tm_min = 0;
            *redotz = true;
            break;
        CASE_fmt_MI
            tm->tm_min += (tm->tm_sec >= 30)?1:0;
            break;
        CASE_fmt_HH
            tm->tm_hour += (tm->tm_min >= 30)?1:0;
            tm->tm_min = 0;
            break;
    }

    tm->tm_sec = 0;
}

Datum
orcl_timestamptz_round(PG_FUNCTION_ARGS)
{
    TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(0);
    TimestampTz result;
    text *fmt = PG_GETARG_TEXT_PP(1);
    int tz;
    fsec_t fsec;
    struct pg_tm tt, *tm = &tt;
    const char *tzn;
    bool redotz = false;

    if (TIMESTAMP_NOT_FINITE(timestamp))
        PG_RETURN_TIMESTAMPTZ(timestamp);

    if (timestamp2tm(timestamp, &tz, tm, &fsec, &tzn, NULL) != 0)
        ereport(ERROR,
                    (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                     errmsg("timestamp out of range")));

    tm_round(tm, fmt, &redotz);

    if (redotz)
        tz = DetermineTimeZoneOffset(tm, get_session_timezone(fcinfo));

    if (tm2timestamp(tm, fsec, &tz, &result) != 0)
        ereport(ERROR,
                (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                 errmsg("timestamp out of range")));

    PG_RETURN_TIMESTAMPTZ(result);
}

Datum
orcl_timestamp_trunc(PG_FUNCTION_ARGS)
{
    Timestamp timestamp = PG_GETARG_TIMESTAMP(0);
    Timestamp result;
    text *fmt = PG_GETARG_TEXT_PP(1);
    fsec_t fsec;
    struct pg_tm tt, *tm = &tt;
    bool redotz = false;

    if (TIMESTAMP_NOT_FINITE(timestamp))
        PG_RETURN_TIMESTAMP(timestamp);

    if (timestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL) != 0)
        ereport(ERROR,
                    (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                     errmsg("timestamp out of range")));

    tm_trunc(tm, fmt, &redotz);
    fsec = 0;

    if (tm2timestamp(tm, fsec, NULL, &result) != 0)
        ereport(ERROR,
                (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                 errmsg("timestamp out of range")));

    PG_RETURN_TIMESTAMP(result);
}

Datum
orcl_timestamp_round(PG_FUNCTION_ARGS)
{
    Timestamp timestamp = PG_GETARG_TIMESTAMP(0);
    Timestamp result;
    text *fmt = PG_GETARG_TEXT_PP(1);
    fsec_t fsec;
    struct pg_tm tt, *tm = &tt;
    bool    redotz = false;

    if (TIMESTAMP_NOT_FINITE(timestamp))
        PG_RETURN_TIMESTAMP(timestamp);

    if (timestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL) != 0)
        ereport(ERROR,
                    (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                     errmsg("timestamp out of range")));

    tm_round(tm, fmt, &redotz);

    if (tm2timestamp(tm, fsec, NULL, &result) != 0)
        ereport(ERROR,
                (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                 errmsg("timestamp out of range")));

    PG_RETURN_TIMESTAMP(result);
}

static Interval *
timezone_get_interval(const char *tzname)
{
    char *lowzone;
    int val;
    pg_tz *tzp;
    int type;
    struct pg_tm tm;
    Interval *resInterval;
    int gmtoffset;

    /* DecodeTimezoneAbbrev requires lowercase input */
    lowzone = downcase_truncate_identifier(tzname,
                                           strlen(tzname),
                                           false);

    type = DecodeTimezoneAbbrev(0, lowzone, &val, &tzp);
    switch (type)
    {
        case TZ:
        case DTZ:
            gmtoffset = val;
            break;
        case DYNTZ:
            {
                /* Determine the current meaning of the abbrev */
                TimestampTz now;
                int            isdst;

                now = GetCurrentTransactionStartTimestamp();
                gmtoffset = -DetermineTimeZoneAbbrevOffsetTS(now,
                                                             tzname,
                                                             tzp,
                                                             &isdst);
                break;
            }
        default:
            {
                /* try it as a full zone name */
                tzp = pg_tzset(tzname);
                if (tzp)
                {
                    /* Apply the timezone change */
                    struct pg_tm     tm;
                    fsec_t            fsec;
                    int             tz;

                    if (timestamp2tm(GetCurrentTransactionStartTimestamp(),
                                     &tz, &tm, &fsec, NULL, tzp) != 0)
                        ereport(ERROR,
                                (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                                 errmsg("timestamp out of range")));
                    gmtoffset = tm.tm_gmtoff;
                }
                else
                {
                    ereport(ERROR,
                            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                             errmsg("time zone \"%s\" not recognized", tzname)));
                }
            }
            break;
    }

    MemSet(&tm, 0, sizeof(struct pg_tm));
    tm.tm_sec = gmtoffset;
    resInterval = (Interval *) palloc(sizeof(Interval));
    tm2interval(&tm, 0, resInterval);

    return resInterval;
}

Datum
orcl_dbtimezone(PG_FUNCTION_ARGS)
{
    const char *tzval;

    tzval = GetConfigOptionResetString("TimeZone");
    Assert(tzval != NULL);

    PG_RETURN_INTERVAL_P(timezone_get_interval(tzval));
}

Datum
orcl_sessiontimezone(PG_FUNCTION_ARGS)
{
    const char *tzn;

    tzn = pg_get_timezone_name(session_timezone);
    if (tzn != NULL)
        PG_RETURN_INTERVAL_P(timezone_get_interval(tzn));

    PG_RETURN_NULL();
}

static Datum
orcl_to_interval(Datum str, int typmod)
{
    Datum result;
    int old_IntervalStyle = IntervalStyle;

    PG_TRY();
    {
        IntervalStyle = INTSTYLE_SQL_STANDARD;
        result = DirectFunctionCall3(interval_in,
                    str,
                    ObjectIdGetDatum(InvalidOid),
                    Int32GetDatum(INTERVAL_TYPMOD(INTERVAL_FULL_PRECISION, typmod)));
    }
    PG_CATCH();
    {
        IntervalStyle = old_IntervalStyle;
        PG_RE_THROW();
    }
    PG_END_TRY();

    IntervalStyle = old_IntervalStyle;

    return result;
}

#if 0
static void
orcl_interval_round_month(Interval * interval)
{
    if (interval != NULL)
    {
        interval->day += (interval->time + USECS_PER_DAY/2)/USECS_PER_DAY;
        interval->month += (interval->day + DAYS_PER_MONTH/2)/DAYS_PER_MONTH;
        interval->day = 0;
        interval->time = 0;
    }
}

static void
orcl_interval_round_day(Interval * interval)
{
    if (interval != NULL)
    {
        interval->day += (interval->time + USECS_PER_DAY/2)/USECS_PER_DAY;
        interval->time = 0;
    }
}
#endif

Datum
orcl_numtoyminterval(PG_FUNCTION_ARGS)
{
    char*        cnum = DatumGetCString(DirectFunctionCall1(numeric_out, PG_GETARG_DATUM(0)));
    text*        unit = PG_GETARG_TEXT_PP(1);
    char*        cunit = text_to_cstring(unit);
    text*        cinterval;
    Datum        result;

    if (pg_strcasecmp(cunit, "YEAR") == 0)
    {
        cinterval = DatumGetTextPP(DirectFunctionCall2(textcat, CStringGetTextDatum(cnum), CStringGetTextDatum(" YEAR")));
    } 
    else if (pg_strcasecmp(cunit, "MONTH") == 0)
    {
        cinterval = DatumGetTextPP(DirectFunctionCall2(textcat, CStringGetTextDatum(cnum), CStringGetTextDatum(" MONTH")));
    } 
    else
    {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("Invalid interval unit for function \"numtoyminterval\": '%s'", cunit),
                    errhint("Valid interval unit can be 'YEAR' or 'MONTH', case-insensitive.")));
    }

    result = DirectFunctionCall3(interval_in,
                                 CStringGetDatum(text_to_cstring(cinterval)), 
                                 ObjectIdGetDatum(InvalidOid), 
                                 Int32GetDatum(-1));
    return result;
}

Datum
orcl_numtodsinterval(PG_FUNCTION_ARGS)
{
    char*        cnum = DatumGetCString(DirectFunctionCall1(numeric_out, PG_GETARG_DATUM(0)));
    text*        unit = PG_GETARG_TEXT_PP(1);
    char*        cunit = text_to_cstring(unit);
    text*        cinterval;
    Datum        result;

    if (pg_strcasecmp(cunit, "DAY") == 0)
    {
        cinterval = DatumGetTextPP(DirectFunctionCall2(textcat, CStringGetTextDatum(cnum), CStringGetTextDatum(" DAY")));
    } 
    else if (pg_strcasecmp(cunit, "HOUR") == 0)
    {
        cinterval = DatumGetTextPP(DirectFunctionCall2(textcat, CStringGetTextDatum(cnum), CStringGetTextDatum(" HOUR")));
    } 
    else if (pg_strcasecmp(cunit, "MINUTE") == 0)
    {
        cinterval = DatumGetTextPP(DirectFunctionCall2(textcat, CStringGetTextDatum(cnum), CStringGetTextDatum(" MINUTE")));
    }
    else if (pg_strcasecmp(cunit, "SECOND") == 0)
    {
        cinterval = DatumGetTextPP(DirectFunctionCall2(textcat, CStringGetTextDatum(cnum), CStringGetTextDatum(" SECOND")));
    } 
    else
    {
        ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid interval unit for function \"numtodsinterval\": '%s'",cunit),
                        errhint("Valid interval unit can be 'DAY', 'HOUR', 'MINUTE' or 'SECOND', case-insensitive.")));
    }

    result = DirectFunctionCall3(interval_in, 
                                 CStringGetDatum(text_to_cstring(cinterval)),
                                 ObjectIdGetDatum(InvalidOid),
                                 Int32GetDatum(-1));
    return result;
}

Datum
orcl_to_yminterval(PG_FUNCTION_ARGS)
{
    text*        val = PG_GETARG_TEXT_PP(0);
    char*        str = text_to_cstring(val);
    int            typmod;
    Datum        result;

    typmod = INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH);

    result = orcl_to_interval(CStringGetDatum(str), typmod);
    pfree(str);

    return result;
}

Datum
orcl_to_dsinterval(PG_FUNCTION_ARGS)
{
    text*        val = PG_GETARG_TEXT_PP(0);
    char*        str = text_to_cstring(val);
    int            typmod;
    Datum        result;

    typmod = INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) |
             INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND);

    result = orcl_to_interval(CStringGetDatum(str), typmod);
    pfree(str);

    return result;
}
