/*
 * Copyright 2015 David A. Boyuka II
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
 */

/*
 * myopts.c
 *
 *  Created on: Dec 5, 2013
 *      Author: David A. Boyuka II
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <assert.h>
#include <getopt.h>

#include "pique/util/myopts.h"

_Bool parse_args(int *argc_ptr, char ***argv_ptr, myoption opts[]) {
    int nopts;
    for (nopts = 0; opts[nopts].flagname; nopts++) {
        const char *fn = opts[nopts].flagname;
        if (!fn || strlen(fn) == 0) {
            fprintf(stderr, "Error: option flag specified with null or empty name\n");
            return false;
        }

        const int hasarg = opts[nopts].hasarg;
        if (hasarg == no_argument || hasarg == optional_argument) {
            if (!opts[nopts].fixedval) {
                fprintf(stderr, "Error: option flag %s has no/an optional argument, but has no fixed value specified\n");
                return false;
            }
        }
    }

    myoption *short_opt_to_opt[128];

    char short_opts_str[nopts * 3 + 1];
    struct option long_opts[nopts];

    int nshortopts = 0, nlongopts = 0;
    char *cur_short_opt = short_opts_str;
    struct option *cur_long_opt = long_opts;

    int returned_longopt;

    for (int i = 0; i < nopts; i++) {
        myoption *cur_opt = &opts[i];
        if (strlen(cur_opt->flagname) == 1) {
            nshortopts++;

            const char shortopt_char = cur_opt->flagname[0];

            *cur_short_opt++ = shortopt_char;
            if (cur_opt->hasarg == required_argument) {
                *cur_short_opt++ = ':';
            } else if (cur_opt->hasarg == optional_argument) {
                *cur_short_opt++ = ':';
                *cur_short_opt++ = ':';
            }

            short_opt_to_opt[shortopt_char] = cur_opt;
        } else {
            nlongopts++;
            *cur_long_opt++ =
                (struct option){
                    .name = cur_opt->flagname,
                    .has_arg = cur_opt->hasarg,
                    .flag = &returned_longopt,
                    .val = i,
                };
        };
    }

    *cur_short_opt++ = '\0';

    int option_index = 0;
    while (1) {
        int c = getopt_long(*argc_ptr, *argv_ptr, short_opts_str, long_opts, &option_index);

        /* Detect the end of the options. */
        if (c == -1)
            break;

        myoption *flag_option;

        switch (c) {
        case 0:
            flag_option = &opts[option_index];
            break;

        case '?':
        case ':':
            /* getopt_long already printed an error message. */
            break;

        default:
            // Short option
            flag_option = short_opt_to_opt[c];
        }

        const int hasarg = flag_option->hasarg;
        void *output = flag_option->output;

        if (hasarg == no_argument || (hasarg == optional_argument && !optarg)) {
            const void *input = flag_option->fixedval;

            switch (flag_option->type) {
            case OPTION_TYPE_INT:
                *(int*)output = *(const int*)input;
                break;
            case OPTION_TYPE_INT64:
                *(int64_t*)output = *(const int64_t*)input;
                break;
            case OPTION_TYPE_UINT64:
                *(uint64_t*)output = *(const uint64_t*)input;
                break;
            case OPTION_TYPE_DOUBLE:
                *(double*)output = *(const double*)input;
                break;
            case OPTION_TYPE_STRING:
                *(const char**)output = (const char*)input;
                break;
            case OPTION_TYPE_BOOLEAN:
                *(_Bool*)output = (input ? *(const _Bool*)input : true);
            }
        } else {
            assert(optarg);

            switch (flag_option->type) {
            case OPTION_TYPE_INT:
                *(int*)output = atoi(optarg);
                break;
            case OPTION_TYPE_INT64:
                *(int64_t*)output = strtoll(optarg, NULL, 0);
                break;
            case OPTION_TYPE_UINT64:
                *(uint64_t*)output = strtoull(optarg, NULL, 0);
                break;
            case OPTION_TYPE_DOUBLE:
                *(double*)output = strtod(optarg, NULL);
                break;
            case OPTION_TYPE_STRING:
                *(const char**)output = optarg;
                break;
            case OPTION_TYPE_BOOLEAN:
                *(_Bool*)output = (strcasecmp(optarg, "true") == 0);
                break;
            }
        }
    }

    *argc_ptr -= optind;
    *argv_ptr += optind;

    return true;
}

