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
 * setop-action-tables.h
 *
 *  Created on: Jan 10, 2014
 *      Author: David A. Boyuka II
 */

#pragma once

#include <stdbool.h>

typedef enum {
    NO_OP,

    DELETE_1,
    COPY_1,
    COMPLEMENT_1,

    DELETE_2,
    COPY_2,
    COMPLEMENT_2,

    INTERSECT,
    UNION,
    DIFFERENCE,
    SYM_DIFFERENCE,
} cblq_code_action_t;

extern const char *SET_OP_NAMES[];

typedef struct {
    int output; // 2 bits = 4 values, only 3 needed
    cblq_code_action_t action; // 4 bits = 16 ops, only the 11 above needed
} action_table_entry_t;

extern const action_table_entry_t UNARY_ACTION_TABLES[][3];
extern const action_table_entry_t BINARY_ACTION_TABLES[][3][3];



typedef enum {
	N_INFEAS = 0, // placeholder action for when transitioning from a state that should never be occupied
	N_DELETE,
	N_UNION,
	N_INTER,
	N_DIFF,     N_CDIFF,
	N_SYMDIFF, N_CSYMDIFF,
} nary_action_t;

typedef unsigned char nary_action_compact_t; // Smallest integer type that can hold an nary_action_t

typedef struct {
    nary_action_t action; // The nary action produced by this entry
    int output;           // The CBLQ code produced by this entry
} nary_action_table_entry_t;

// [action][prev code][current code]
extern const nary_action_table_entry_t NARY_ACTION_TABLE[][3][3];
extern const nary_action_t NARY_ACTION_INITIAL_CODE[];
extern const char NARY_ACTION_CHAR_CODE[];
