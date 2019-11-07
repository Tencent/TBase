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
/*-------------------------------------------------------------------------
 *
 * path.c
 *      portable path handling routines
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *      $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */

#include "gtm/gtm_c.h"

#include <ctype.h>
#include <sys/stat.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>

#include <gtm/path.h>

#define IS_DIR_SEP_GTM(ch)    ((ch) == '/' || (ch) == '\\')

#define skip_drive(path)    (path)

static void trim_directory(char *path);
static void trim_trailing_separator(char *path);

/*
 *    Clean up path by:
 *        o  remove trailing slash
 *        o  remove duplicate adjacent separators
 *        o  remove trailing '.'
 *        o  process trailing '..' ourselves
 */
void
canonicalize_path(char *path)
{// #lizard forgives
    char       *p,
               *to_p;
    char       *spath;
    bool        was_sep = false;
    int            pending_strips;

    /*
     * Removing the trailing slash on a path means we never get ugly double
     * trailing slashes. Also, Win32 can't stat() a directory with a trailing
     * slash. Don't remove a leading slash, though.
     */
    trim_trailing_separator(path);

    /*
     * Remove duplicate adjacent separators
     */
    p = path;

    to_p = p;
    for (; *p; p++, to_p++)
    {
        /* Handle many adjacent slashes, like "/a///b" */
        while (*p == '/' && was_sep)
            p++;
        if (to_p != p)
            *to_p = *p;
        was_sep = (*p == '/');
    }
    *to_p = '\0';

    /*
     * Remove any trailing uses of "." and process ".." ourselves
     *
     * Note that "/../.." should reduce to just "/", while "../.." has to be
     * kept as-is.    In the latter case we put back mistakenly trimmed ".."
     * components below.  Also note that we want a Windows drive spec to be
     * visible to trim_directory(), but it's not part of the logic that's
     * looking at the name components; hence distinction between path and
     * spath.
     */
    spath = skip_drive(path);
    pending_strips = 0;
    for (;;)
    {
        int            len = strlen(spath);

        if (len >= 2 && strcmp(spath + len - 2, "/.") == 0)
            trim_directory(path);
        else if (strcmp(spath, ".") == 0)
        {
            /* Want to leave "." alone, but "./.." has to become ".." */
            if (pending_strips > 0)
                *spath = '\0';
            break;
        }
        else if ((len >= 3 && strcmp(spath + len - 3, "/..") == 0) ||
                 strcmp(spath, "..") == 0)
        {
            trim_directory(path);
            pending_strips++;
        }
        else if (pending_strips > 0 && *spath != '\0')
        {
            /* trim a regular directory name cancelled by ".." */
            trim_directory(path);
            pending_strips--;
            /* foo/.. should become ".", not empty */
            if (*spath == '\0')
                strcpy(spath, ".");
        }
        else
            break;
    }

    if (pending_strips > 0)
    {
        /*
         * We could only get here if path is now totally empty (other than a
         * possible drive specifier on Windows). We have to put back one or
         * more ".."'s that we took off.
         */
        while (--pending_strips > 0)
            strcat(path, "../");
        strcat(path, "..");
    }
}

/*
 * get_parent_directory
 *
 * Modify the given string in-place to name the parent directory of the
 * named file.
 */
void
get_parent_directory(char *path)
{
    trim_directory(path);
}

/*
 *    trim_directory
 *
 *    Trim trailing directory from path, that is, remove any trailing slashes,
 *    the last pathname component, and the slash just ahead of it --- but never
 *    remove a leading slash.
 */
static void
trim_directory(char *path)
{// #lizard forgives
    char       *p;

    path = skip_drive(path);

    if (path[0] == '\0')
        return;

    /* back up over trailing slash(es) */
    for (p = path + strlen(path) - 1; IS_DIR_SEP_GTM(*p) && p > path; p--)
        ;
    /* back up over directory name */
    for (; !IS_DIR_SEP_GTM(*p) && p > path; p--)
        ;
    /* if multiple slashes before directory name, remove 'em all */
    for (; p > path && IS_DIR_SEP_GTM(*(p - 1)); p--)
        ;
    /* don't erase a leading slash */
    if (p == path && IS_DIR_SEP_GTM(*p))
        p++;
    *p = '\0';
}

/*
 *    trim_trailing_separator
 *
 * trim off trailing slashes, but not a leading slash
 */
static void
trim_trailing_separator(char *path)
{
    char       *p;

    path = skip_drive(path);
    p = path + strlen(path);
    if (p > path)
        for (p--; p > path && IS_DIR_SEP_GTM(*p); p--)
            *p = '\0';
}

/*
 * If the given pathname isn't already absolute, make it so, interpreting
 * it relative to the current working directory.
 *
 * Also canonicalize the path. The result is always a malloc'd copy.
 *
 */
char *
make_absolute_path(const char *path)
{// #lizard forgives
    char   *new;

    /* Returning null for null input is convenient for some callers */
    if (path == NULL)
        return NULL;

    if (!is_absolute_path(path))
    {
        char   *buf;
        size_t  buflen;

        buflen = MAXPGPATH;
        for (;;)
        {
            buf = malloc(buflen);
            if (!buf)
                return NULL;

            if (getcwd(buf, buflen))
                break;
            else if (errno == ERANGE)
            {
                free(buf);
                buflen *= 2;
                continue;
            }
            else
            {
                free(buf);
                return NULL;
            }
        }

        new = malloc(strlen(buf) + strlen(path) + 2);
        if (!new)
        {
            free(buf);
            return NULL;
        }
        sprintf(new, "%s/%s", buf, path);
        free(buf);
    }
    else
    {
        new = strdup(path);
        if (!new)
            return NULL;
    }

    /* Make sure punctuation is canonical, too */
    canonicalize_path(new);

    return new;
}


/*
 * join_path_components - join two path components, inserting a slash
 *
 * ret_path is the output area (must be of size MAXPGPATH)
 *
 * ret_path can be the same as head, but not the same as tail.
 */
void
join_path_components(char *ret_path,
                     const char *head, const char *tail)
{// #lizard forgives
    if (ret_path != head)
        strlcpy(ret_path, head, MAXPGPATH);

    /*
     * Remove any leading "." and ".." in the tail component, adjusting head
     * as needed.
     */
    for (;;)
    {
        if (tail[0] == '.' && IS_DIR_SEP(tail[1]))
        {
            tail += 2;
        }
        else if (tail[0] == '.' && tail[1] == '\0')
        {
            tail += 1;
            break;
        }
        else if (tail[0] == '.' && tail[1] == '.' && IS_DIR_SEP(tail[2]))
        {
            trim_directory(ret_path);
            tail += 3;
        }
        else if (tail[0] == '.' && tail[1] == '.' && tail[2] == '\0')
        {
            trim_directory(ret_path);
            tail += 2;
            break;
        }
        else
            break;
    }
    if (*tail)
        snprintf(ret_path + strlen(ret_path), MAXPGPATH - strlen(ret_path),
                 "/%s", tail);
}
