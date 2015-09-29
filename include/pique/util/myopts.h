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
 * myopts.h
 *
 *  Created on: Dec 5, 2013
 *      Author: David A. Boyuka II
 */

#ifndef MYOPTS_H_
#define MYOPTS_H_

#include <stdbool.h>

typedef enum {
    OPTION_TYPE_INT,
    OPTION_TYPE_INT64,
    OPTION_TYPE_UINT64,
    OPTION_TYPE_DOUBLE,
    OPTION_TYPE_STRING,
    OPTION_TYPE_BOOLEAN,
} OPTION_VALUE_TYPE;

typedef struct {
    const char *flagname;
    int hasarg;
    OPTION_VALUE_TYPE type; // Datatype of the arg

    // If an argument is specified to this flag, its value is set at *output
    // Otherwise, the value at *fixedval is used
    const void *fixedval;
    void *output;
} myoption;

_Bool parse_args(int *argc_ptr, char ***argv_ptr, myoption opts[]);

#endif /* MYOPTS_H_ */
