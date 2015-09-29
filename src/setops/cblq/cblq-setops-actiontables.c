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
 * cblq-setops-actiontables.c
 *
 *  Created on: Jan 10, 2014
 *      Author: David A. Boyuka II
 */

#include <stdint.h>

#include "setops/cblq/cblq-setops-actiontables.h"

static const action_table_entry_t DELETE_1_ACTION_TABLE[3] = {
        { 0, NO_OP },        // Input: 0
        { 0, NO_OP },        // Input: 1
        { 0, DELETE_1 },    // Input: 2
};

static const action_table_entry_t DELETE_2_ACTION_TABLE[3] = {
        { 0, NO_OP },        // Input: 0
        { 0, NO_OP },        // Input: 1
        { 0, DELETE_2 },    // Input: 2
};

static const action_table_entry_t COPY_1_ACTION_TABLE[3] = {
        { 0, NO_OP },    // Input: 0
        { 1, NO_OP },    // Input: 1
        { 2, COPY_1 },    // Input: 2
};

static const action_table_entry_t COPY_2_ACTION_TABLE[3] = {
        { 0, NO_OP },    // Input: 0
        { 1, NO_OP },    // Input: 1
        { 2, COPY_2 },    // Input: 2
};

static const action_table_entry_t COMPLEMENT_1_ACTION_TABLE[3] = {
        { 1, NO_OP },        // Input: 0
        { 0, NO_OP },        // Input: 1
        { 2, COMPLEMENT_1 },// Input: 2
};

static const action_table_entry_t COMPLEMENT_2_ACTION_TABLE[3] = {
        { 1, NO_OP },        // Input: 0
        { 0, NO_OP },        // Input: 1
        { 2, COMPLEMENT_2 },// Input: 2
};

static const action_table_entry_t UNION_ACTION_TABLE[3][3] = {
        //    Right: 0        Right: 1        Right: 2
        {    { 0, NO_OP },    { 1, NO_OP },     { 2, COPY_2 }    }, // Left: 0
        {    { 1, NO_OP },    { 1, NO_OP },     { 1, DELETE_2 }    }, // Left: 1
        {    { 2, COPY_1 },    { 1, DELETE_1 },{ 2, UNION }    }, // Left: 2
};

static const action_table_entry_t INTERSECT_ACTION_TABLE[3][3] = {
        //    Right: 0        Right: 1        Right: 2
        {    { 0, NO_OP },    { 0, NO_OP },     { 0, DELETE_2 }   }, // Left: 0
        {    { 0, NO_OP },    { 1, NO_OP },     { 2, COPY_2 }     }, // Left: 1
        {    { 0, DELETE_1 }, { 2, COPY_1 },    { 2, INTERSECT }  }, // Left: 2
};

static const action_table_entry_t DIFFERENCE_ACTION_TABLE[3][3] = {
        //    Right: 0        Right: 1        Right: 2
        {    { 0, NO_OP },    { 0, NO_OP },     { 0, DELETE_2 }        }, // Left: 0
        {    { 1, NO_OP },    { 0, NO_OP },     { 2, COMPLEMENT_2 }    }, // Left: 1
        {    { 2, COPY_1 },   { 0, DELETE_1 },  { 2, DIFFERENCE }      }, // Left: 2
};

static const action_table_entry_t SYM_DIFFERENCE_ACTION_TABLE[3][3] = {
        //    Right: 0        Right: 1        Right: 2
        {    { 0, NO_OP },    { 1, NO_OP },         { 2, COPY_2 }            }, // Left: 0
        {    { 1, NO_OP },    { 0, NO_OP },         { 2, COMPLEMENT_2 }      }, // Left: 1
        {    { 2, COPY_1 },   { 2, COMPLEMENT_1 },  { 2, SYM_DIFFERENCE }    }, // Left: 2
};

//DELETE_1,
//DELETE_2,
//COPY_1,
//COPY_2,
//COMPLEMENT,
//INTERSECT,
//UNION,
//DIFFERENCE,
//SYM_DIFFERENCE,

const action_table_entry_t UNARY_ACTION_TABLES[][3] = {
        [NO_OP] =  { {-1, NO_OP}, {-1, NO_OP}, {-1, NO_OP}, },

        [DELETE_1] = {
            { -1, NO_OP },       // Input: 0
            { -1, NO_OP },       // Input: 1
            { -1, DELETE_1 },    // Input: 2
        },
        [DELETE_2] = {
            { -1, NO_OP },       // Input: 0
            { -1, NO_OP },       // Input: 1
            { -1, DELETE_2 },    // Input: 2
        },
        [COPY_1] = {
                { 0, NO_OP },    // Input: 0
                { 1, NO_OP },    // Input: 1
                { 2, COPY_1 },   // Input: 2
        },
        [COPY_2] = {
                { 0, NO_OP },    // Input: 0
                { 1, NO_OP },    // Input: 1
                { 2, COPY_2 },   // Input: 2
        },
        [COMPLEMENT_1] = {
                { 1, NO_OP },        // Input: 0
                { 0, NO_OP },        // Input: 1
                { 2, COMPLEMENT_1 }, // Input: 2
        },
        [COMPLEMENT_2] = {
                { 1, NO_OP },        // Input: 0
                { 0, NO_OP },        // Input: 1
                { 2, COMPLEMENT_2 }, // Input: 2
        },
};

const action_table_entry_t BINARY_ACTION_TABLES[][3][3] = {
        [UNION] = {
                //    Right: 0        Right: 1          Right: 2
                {    { 0, NO_OP },    { 1, NO_OP },     { 2, COPY_2 }    }, // Left: 0
                {    { 1, NO_OP },    { 1, NO_OP },     { 1, DELETE_2 }  }, // Left: 1
                {    { 2, COPY_1 },   { 1, DELETE_1 },  { 2, UNION }     }, // Left: 2
        },
        [INTERSECT] = {
                //    Right: 0        Right: 1          Right: 2
                {    { 0, NO_OP },    { 0, NO_OP },     { 0, DELETE_2 }  }, // Left: 0
                {    { 0, NO_OP },    { 1, NO_OP },     { 2, COPY_2 }    }, // Left: 1
                {    { 0, DELETE_1 }, { 2, COPY_1 },    { 2, INTERSECT } }, // Left: 2
        },
        [DIFFERENCE] = {
                //    Right: 0        Right: 1          Right: 2
                {    { 0, NO_OP },    { 0, NO_OP },     { 0, DELETE_2 }     }, // Left: 0
                {    { 1, NO_OP },    { 0, NO_OP },     { 2, COMPLEMENT_2 } }, // Left: 1
                {    { 2, COPY_1 },   { 0, DELETE_1 },  { 2, DIFFERENCE }   }, // Left: 2
        },
        [SYM_DIFFERENCE] = {
                //    Right: 0        Right: 1             Right: 2
                {    { 0, NO_OP },    { 1, NO_OP },        { 2, COPY_2 }         }, // Left: 0
                {    { 1, NO_OP },    { 0, NO_OP },        { 2, COMPLEMENT_2 }   }, // Left: 1
                {    { 2, COPY_1 },   { 2, COMPLEMENT_1 }, { 2, SYM_DIFFERENCE } }, // Left: 2
        },
};

const char *SET_OP_NAMES[] = {
        [NO_OP] = "NO_OP",
        [DELETE_1] = "DELETE_1",
        [DELETE_2] = "DELETE_2",
        [COPY_1] = "COPY_1",
        [COPY_2] = "COPY_2",
        [COMPLEMENT_1] = "COMPLEMENT_1",
        [COMPLEMENT_2] = "COMPLEMENT_2",
        [UNION] = "UNION",
        [INTERSECT] = "INTERSECT",
        [DIFFERENCE] = "DIFFERENCE",
        [SYM_DIFFERENCE] = "SYM_DIFFERENCE",
};



// N-ary action table

const char NARY_ACTION_CHAR_CODE[] = {
		[N_INFEAS]  = '*', // Should never be seen
		[N_DELETE] 	= 'D',
		[N_UNION] 	= 'U',
		[N_INTER]	= 'I',
		[N_DIFF]	= '-',
		[N_CDIFF]	= '~',
		[N_SYMDIFF] = 'X',
		[N_CSYMDIFF] = 'x',
};

const nary_action_t NARY_ACTION_INITIAL_CODE[] = {
		[N_INFEAS]  = 0, // Doesn't matter
		[N_DELETE] 	= 0, // Doesn't matter
		[N_UNION] 	= 0,
		[N_INTER]	= 1,
		[N_DIFF]	= 0,
		[N_CDIFF]	= 1,
		[N_SYMDIFF] = 0,
		[N_CSYMDIFF] = 1,
};

// [action][prev code][current code]
const nary_action_table_entry_t NARY_ACTION_TABLE[][3][3] = {
		[N_DELETE] = {
				//	 Right: 0			Right: 1			Right: 2
				{ 	{ N_DELETE,	0, },	{ N_DELETE,	0 },	{ N_DELETE,	0}	}, // Left: 0
				{ 	{ N_DELETE,	1, },	{ N_DELETE,	1 },	{ N_DELETE,	1}	}, // Left: 1
				{ 	{ N_DELETE,	2, },	{ N_DELETE,	2 },	{ N_DELETE,	2}	}, // Left: 2
		},
		[N_UNION] = {
				//	 Right: 0			Right: 1			Right: 2
				{ 	{ N_UNION, 	0, },	{ N_DELETE,	1 },	{ N_UNION, 	2}	}, // Left: 0
				{ 	{ N_DELETE, 1, },	{ N_DELETE,	1 },	{ N_DELETE,	1}	}, // Left: 1
				{ 	{ N_UNION, 	2, },	{ N_DELETE,	1 },	{ N_UNION, 	2}	}, // Left: 2
		},
		[N_INTER] = {
				//	 Right: 0			Right: 1			Right: 2
				{ 	{ N_DELETE,	0, },	{ N_DELETE,	0 },	{ N_DELETE, 0}	}, // Left: 0
				{ 	{ N_DELETE, 0, },	{ N_INTER,	1 },	{ N_INTER,	2}	}, // Left: 1
				{ 	{ N_DELETE, 0, },	{ N_INTER,	2 },	{ N_INTER, 	2}	}, // Left: 2
		},
		[N_DIFF] = {
				//	 Right: 0			Right: 1			Right: 2
				{ 	{ N_DELETE,	0, },	{ N_CDIFF,	1 },	{ N_DIFF,	2}	}, // Left: 0
				{ 	{ N_INFEAS, 0, },	{ N_INFEAS,	0 },	{ N_INFEAS,	0}	}, // Left: 1
				{ 	{ N_DIFF,	2, },	{ N_DELETE,	0 },	{ N_DIFF, 	2}	}, // Left: 2
		},
		[N_CDIFF] = {
				//	 Right: 0			Right: 1			Right: 2
				{ 	{ N_INFEAS,	0, },	{ N_INFEAS,	0 },	{ N_INFEAS, 0}	}, // Left: 0
				{ 	{ N_CDIFF,	1, },	{ N_DELETE,	0 },	{ N_CDIFF,	2}	}, // Left: 1
				{ 	{ N_CDIFF,  2, },	{ N_DELETE,	0 },	{ N_CDIFF, 	2}	}, // Left: 2
		},
		[N_SYMDIFF] = {
				//	 Right: 0				Right: 1				Right: 2
				{ 	{ N_SYMDIFF,	0, },	{ N_CSYMDIFF,	1 },	{ N_SYMDIFF,	2}	}, // Left: 0
				{ 	{ N_INFEAS,		0, },	{ N_INFEAS,		0 },	{ N_INFEAS,		0}	}, // Left: 1
				{ 	{ N_SYMDIFF,	2, },	{ N_CSYMDIFF,	2 },	{ N_SYMDIFF, 	2}	}, // Left: 2
		},
		[N_CSYMDIFF] = {
				//	 Right: 0				Right: 1				Right: 2
				{ 	{ N_INFEAS,		0, },	{ N_INFEAS,		0 },	{ N_INFEAS,		0}	}, // Left: 0
				{ 	{ N_CSYMDIFF,	1, },	{ N_SYMDIFF,	0 },	{ N_CSYMDIFF,	2}	}, // Left: 1
				{ 	{ N_CSYMDIFF,	2, },	{ N_SYMDIFF,	2 },	{ N_CSYMDIFF, 	2}	}, // Left: 2
		},
};

