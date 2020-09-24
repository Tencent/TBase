/*-------------------------------------------------------------------------
 *
 * bloom.c
 *
 *	  a bloom filter, using murmurhash
 *
 * Copyright (c) 2020-Present TBase development team, Tencent
 *
 *
 * IDENTIFICATION
 *	  src/gtm/common/bloom.c
 *
 *-------------------------------------------------------------------------
 */

#include<limits.h>
#include<stdarg.h>
#include "gtm/gtm_c.h"
#include "gtm/gtm.h"
#include "gtm/bloom.h"
#include "gtm/palloc.h"

#define SETBIT(bitmap, bit) ((bitmap)[(bit)/CHAR_BIT] |= (1<<((bit)%CHAR_BIT)))
#define GETBIT(bitmap, bit) ((bitmap)[(bit)/CHAR_BIT] & (1<<((bit)%CHAR_BIT)))
#define MIX(h,k,m) { k *= m; k ^= k >> r; k *= m; h *= m; h ^= k; }

/*
 * Create a bloom filter, variable parameter is hash seed
 * hash function num depend on seeds
 */
BLOOM *
BloomCreate(int bitmap_size, int nfuncs, ...)
{
    BLOOM *bloom;
    va_list l;
    int i;

    bloom = palloc(sizeof(BLOOM));
    if (NULL == bloom)
    {
        return NULL;
    }

    bloom->bitmap = palloc0( ((bitmap_size + CHAR_BIT - 1) / CHAR_BIT) * sizeof(char));
    if (NULL == bloom->bitmap)
    {
        pfree(bloom);
        return NULL;
    }

    bloom->seeds = (uint32*)palloc(nfuncs * sizeof(uint32));
    if (NULL == bloom->seeds)
    {
        pfree(bloom->bitmap);
        pfree(bloom);
        return NULL;
    }

    va_start(l, nfuncs);
    for(i = 0; i < nfuncs; ++i)
    {
        bloom->seeds[i] = va_arg(l, uint32);
    }
    va_end(l);

    bloom->bitmap_size = bitmap_size;
    bloom->nfuncs = nfuncs;

    return bloom;
}

/*
 * Destroy a bloom filter
 */
int
BloomDestroy(BLOOM *bloom)
{
    pfree(bloom->bitmap);
    pfree(bloom->seeds);
    pfree(bloom);

    return 0;
}

/*
 * Reset bloom filter's bitmap
 */
void
BloomReset(BLOOM *bloom)
{
    MemSet(bloom->bitmap, 0, ((bloom->bitmap_size + CHAR_BIT - 1) / CHAR_BIT) * sizeof(char));
}

/*
 * Add an item into bloom filter
 */
void
BloomAdd(BLOOM *bloom, const char *s, int len)
{
    int i;
    for(i = 0; i < bloom->nfuncs; ++i)
    {
        SETBIT(bloom->bitmap, MurmurHash2(s, len, bloom->seeds[i]) % bloom->bitmap_size);
    }
}

/*
 * Check if the item exist
 */
bool
BloomCheck(BLOOM *bloom, const char *s, int len)
{
    int i;

    for(i = 0; i < bloom->nfuncs; ++i)
    {
        if(!(GETBIT(bloom->bitmap, MurmurHash2(s, len, bloom->seeds[i]) % bloom->bitmap_size)))
        {
            return false;
        }
    }

    return true;
}

/*
 * Check if the item exist, if not exist, add the item into bloom
 */
bool
BloomCheckAndAdd(BLOOM *bloom, const char *s, int len)
{
    int i, j;
    uint32 hash;
    bool exist = true;
    for(i = 0; i < bloom->nfuncs; ++i)
    {
        hash = MurmurHash2(s, len, bloom->seeds[i]) % bloom->bitmap_size;
        if(!(GETBIT(bloom->bitmap, hash)))
        {
            exist = false;
            SETBIT(bloom->bitmap, hash);
            for (j = i + 1; j < bloom->nfuncs; ++j)
            {
                hash = MurmurHash2(s, len, bloom->seeds[j]) % bloom->bitmap_size;
                SETBIT(bloom->bitmap, hash);
            }
            break;
        }
    }
    return exist;
}

/*
 * Murmurhash function
 */
uint32_t
MurmurHash2(const void * key, int len, uint32_t seed)
{
    const uint32_t m = 0x5bd1e995;
    const int32_t r = 24;
    const uint8_t * data = (const uint8_t *)key;
    uint32_t h = seed ^ len;
    uint8_t align = (uintptr_t)data & 3;

    if(align && (len >= 4))
    {
        /* Pre-load the temp registers */
        uint32_t t = 0, d = 0;
        int32_t sl;
        int32_t sr;

        switch(align)
        {
            case 1: t |= data[2] << 16;
            case 2: t |= data[1] << 8;
            case 3: t |= data[0];
        }

        t <<= (8 * align);

        data += 4-align;
        len -= 4-align;

        sl = 8 * (4-align);
        sr = 8 * align;

        /* Mix */

        while(len >= 4)
        {
            uint32_t k;

            d = *(uint32_t *)data;
            t = (t >> sr) | (d << sl);

            k = t;

            MIX(h,k,m);

            t = d;

            data += 4;
            len -= 4;
        }

        /* Handle leftover data in temp registers */

        d = 0;

        if(len >= align)
        {
            uint32_t k;

            switch(align)
            {
                case 3: d |= data[2] << 16;
                case 2: d |= data[1] << 8;
                case 1: d |= data[0];
            }

            k = (t >> sr) | (d << sl);
            MIX(h,k,m);

            data += align;
            len -= align;

            /* ----------
             * Handle tail bytes */

            switch(len)
            {
                case 3: h ^= data[2] << 16;
                case 2: h ^= data[1] << 8;
                case 1: h ^= data[0]; h *= m;
            };
        }
        else
        {
            switch(len)
            {
                case 3: d |= data[2] << 16;
                case 2: d |= data[1] << 8;
                case 1: d |= data[0];
                case 0: h ^= (t >> sr) | (d << sl); h *= m;
            }
        }

        h ^= h >> 13;
        h *= m;
        h ^= h >> 15;

        return h;
    }
    else
    {
        while(len >= 4)
        {
            uint32_t k = *(uint32_t *)data;

            MIX(h,k,m);

            data += 4;
            len -= 4;
        }

        /* ----------
         * Handle tail bytes */

        switch(len)
        {
            case 3: h ^= data[2] << 16;
            case 2: h ^= data[1] << 8;
            case 1: h ^= data[0]; h *= m;
        };

        h ^= h >> 13;
        h *= m;
        h ^= h >> 15;

        return h;
    }
}
