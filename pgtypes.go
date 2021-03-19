package pglogrepl

import (
	"fmt"
)

type PgType struct {
	Oid int
	ArrayTypeOid int
	Typname string
	Typlen int
	Alias string
}

func (t *PgType) String() string {
	return t.Typname
}

// GetPgTypeById searches type by oid by next algorithm
// 1. search into the PgTypes map
// 2. if not exists then search into the PgArrTypes map
// 3. if not exists too, then returns PgUnknownType
//
// returns PgType and flag is_array
func GetPgTypeById(oid int) (PgType, bool) {
	if pgt, ok := PgTypes[oid]; ok {
		return pgt, false
	}

	if id, ok := PgArrTypes[oid]; ok {
		return PgTypes[id], true
	}

	return PgUnknownType, false
}

var PgUnknownType = PgType{
	Oid:          705,
	ArrayTypeOid: 0,
	Typname:      "unknown",
	Typlen:       -2,
}

func init() {
	for k, v := range PgTypes {
		if k != v.Oid {
			panic(fmt.Sprintf("broken types, expected key are equal oid. But got k: %d and oid: %d", k, v.Oid))
		}

		PgArrTypes[v.ArrayTypeOid] = v.Oid
	}
}

var PgArrTypes = map[int]int{}

var PgTypes = map[int]PgType {
	16: {
		Oid:          16,
		ArrayTypeOid: 1000,
		Typname:      "bool",
		Typlen:       1,
	},
	17: {
		Oid: 17,
		ArrayTypeOid: 1001,
		Typname: "bytea",
		Typlen: -1,
	},
	18: {
		Oid: 18,
		ArrayTypeOid: 1002,
		Typname: "char",
		Typlen: 1,
	},
	19: {
		Oid:          19,
		ArrayTypeOid: 1003,
		Typname:      "name",
		Typlen:       64,
	},
	20: {
		Oid:          20,
		ArrayTypeOid: 1016,
		Typname:      "int8",
		Typlen:       8,
		Alias:        "bigint",
	},
	21: {
		Oid:          21,
		ArrayTypeOid: 1005,
		Typname:      "int2",
		Typlen:       2,
		Alias:        "smallint",
	},
	22: {
		Oid:          22,
		ArrayTypeOid: 1006,
		Typname:      "int2vector",
		Typlen:       -1,
	},
	23: {
		Oid:          23,
		ArrayTypeOid: 1007,
		Typname:      "int4",
		Typlen:       4,
		Alias:        "integer",
	},
	24: {
		Oid:          24,
		ArrayTypeOid: 1008,
		Typname:      "regproc",
		Typlen:       4,
	},
	25: {
		Oid:          25,
		ArrayTypeOid: 1009,
		Typname:      "text",
		Typlen:       -1,
	},
	26: {
		Oid:          26,
		ArrayTypeOid: 1028,
		Typname:      "oid",
		Typlen:       4,
	},
	27: {
		Oid:          27,
		ArrayTypeOid: 1010,
		Typname:      "tid",
		Typlen:       6,
	},
	28: {
		Oid:          28,
		ArrayTypeOid: 1011,
		Typname:      "xid",
		Typlen:       4,
	},
	29: {
		Oid:          29,
		ArrayTypeOid: 1012,
		Typname:      "cid",
		Typlen:       4,
	},
	30: {
		Oid:          30,
		ArrayTypeOid: 1013,
		Typname:      "oidvector",
		Typlen:       -1,
	},
	114: {
		Oid:          114,
		ArrayTypeOid: 199,
		Typname:      "json",
		Typlen:       -1,
	},
	142: {
		Oid:          142,
		ArrayTypeOid: 143,
		Typname:      "xml",
		Typlen:       -1,
	},
	600: {
		Oid:          600,
		ArrayTypeOid: 1017,
		Typname:      "point",
		Typlen:       16,
	},
	601: {
		Oid:          601,
		ArrayTypeOid: 1018,
		Typname:      "lseg",
		Typlen:       32,
	},
	602: {
		Oid:          602,
		ArrayTypeOid: 1019,
		Typname:      "path",
		Typlen:       -1,
	},
	603: {
		Oid:          603,
		ArrayTypeOid: 1020,
		Typname:      "box",
		Typlen:       32,
	},
	604: {
		Oid:          604,
		ArrayTypeOid: 1027,
		Typname:      "polygon",
		Typlen:       -1,
	},
	628: {
		Oid:          628,
		ArrayTypeOid: 629,
		Typname:      "line",
		Typlen:       24,
	},
	700: {
		Oid:          700,
		ArrayTypeOid: 1021,
		Typname:      "float4",
		Typlen:       4,
		Alias:        "float",
	},
	701: {
		Oid:          701,
		ArrayTypeOid: 1022,
		Typname:      "float8",
		Typlen:       8,
		Alias:        "double",
	},
	718: {
		Oid:          718,
		ArrayTypeOid: 719,
		Typname:      "circle",
		Typlen:       24,
	},
	790: {
		Oid:          790,
		ArrayTypeOid: 791,
		Typname:      "money",
		Typlen:       8,
	},
	829: {
		Oid:          829,
		ArrayTypeOid: 1040,
		Typname:      "macaddr",
		Typlen:       6,
	},
	869: {
		Oid:          869,
		ArrayTypeOid: 1041,
		Typname:      "inet",
		Typlen:       -1,
	},
	650: {
		Oid:          650,
		ArrayTypeOid: 651,
		Typname:      "cidr",
		Typlen:       -1,
	},
	774: {
		Oid:          774,
		ArrayTypeOid: 775,
		Typname:      "macaddr8",
		Typlen:       8,
	},
	1033: {
		Oid:          1033,
		ArrayTypeOid: 1034,
		Typname:      "aclitem",
		Typlen:       12,
	},
	1042: {
		Oid:          1042,
		ArrayTypeOid: 1014,
		Typname:      "bpchar",
		Typlen:       -1,
	},
	1043: {
		Oid:          1043,
		ArrayTypeOid: 1015,
		Typname:      "varchar",
		Typlen:       -1,
	},
	1082: {
		Oid:          1082,
		ArrayTypeOid: 1182,
		Typname:      "date",
		Typlen:       4,
	},
	1083: {
		Oid:          1083,
		ArrayTypeOid: 1183,
		Typname:      "time",
		Typlen:       8,
	},
	1114: {
		Oid:          1114,
		ArrayTypeOid: 1115,
		Typname:      "timestamp",
		Typlen:       8,
	},
	1184: {
		Oid:          1184,
		ArrayTypeOid: 1185,
		Typname:      "timestamptz",
		Typlen:       8,
		Alias:        "timestamp",
	},
	1186: {
		Oid:          1186,
		ArrayTypeOid: 1187,
		Typname:      "interval",
		Typlen:       16,
	},
	1266: {
		Oid:          1266,
		ArrayTypeOid: 1270,
		Typname:      "timetz",
		Typlen:       12,
		Alias:        "time",
	},
	1560: {
		Oid:          1560,
		ArrayTypeOid: 1561,
		Typname:      "bit",
		Typlen:       -1,
	},
	1562: {
		Oid:          1562,
		ArrayTypeOid: 1563,
		Typname:      "varbit",
		Typlen:       -1,
	},
	1700: {
		Oid:          1700,
		ArrayTypeOid: 1231,
		Typname:      "numeric",
		Typlen:       -1,
		Alias:        "decimal",
	},
	1790: {
		Oid:          1790,
		ArrayTypeOid: 2201,
		Typname:      "refcursor",
		Typlen:       -1,
	},
	2202: {
		Oid:          2202,
		ArrayTypeOid: 2207,
		Typname:      "regprocedure",
		Typlen:       4,
	},
	2203: {
		Oid:          2203,
		ArrayTypeOid: 2208,
		Typname:      "regoper",
		Typlen:       4,
	},
	2204: {
		Oid:          2204,
		ArrayTypeOid: 2209,
		Typname:      "regoperator",
		Typlen:       4,
	},
	2205: {
		Oid:          2205,
		ArrayTypeOid: 2210,
		Typname:      "regclass",
		Typlen:       4,
	},
	2206: {
		Oid:          2206,
		ArrayTypeOid: 2211,
		Typname:      "regtype",
		Typlen:       4,
	},
	4096: {
		Oid:          4096,
		ArrayTypeOid: 4097,
		Typname:      "regrole",
		Typlen:       4,
	},
	4089: {
		Oid:          4089,
		ArrayTypeOid: 4090,
		Typname:      "regnamespace",
		Typlen:       4,
	},
	2950: {
		Oid:          2950,
		ArrayTypeOid: 2951,
		Typname:      "uuid",
		Typlen:       16,
	},
	3220: {
		Oid:          3220,
		ArrayTypeOid: 3221,
		Typname:      "pg_lsn",
		Typlen:       8,
	},
	3614: {
		Oid:          3614,
		ArrayTypeOid: 3643,
		Typname:      "tsvector",
		Typlen:       -1,
	},
	3642: {
		Oid:          3642,
		ArrayTypeOid: 3644,
		Typname:      "gtsvector",
		Typlen:       -1,
	},
	3615: {
		Oid:          3615,
		ArrayTypeOid: 3645,
		Typname:      "tsquery",
		Typlen:       -1,
	},
	3734: {
		Oid:          3734,
		ArrayTypeOid: 3735,
		Typname:      "regconfig",
		Typlen:       4,
	},
	3769: {
		Oid:          3769,
		ArrayTypeOid: 3770,
		Typname:      "regdictionary",
		Typlen:       4,
	},
	3802: {
		Oid:          3802,
		ArrayTypeOid: 3807,
		Typname:      "jsonb",
		Typlen:       -1,
	},
	4072: {
		Oid:          4072,
		ArrayTypeOid: 4073,
		Typname:      "jsonpath",
		Typlen:       -1,
	},
	2970: {
		Oid:          2970,
		ArrayTypeOid: 2949,
		Typname:      "txid_snapshot",
		Typlen:       -1,
	},
	3904: {
		Oid:          3904,
		ArrayTypeOid: 3905,
		Typname:      "int4range",
		Typlen:       -1,
	},
	3906: {
		Oid:          3906,
		ArrayTypeOid: 3907,
		Typname:      "numrange",
		Typlen:       -1,
	},
	3908: {
		Oid:          3908,
		ArrayTypeOid: 3909,
		Typname:      "tsrange",
		Typlen:       -1,
	},
	3910: {
		Oid:          3910,
		ArrayTypeOid: 3911,
		Typname:      "tstzrange",
		Typlen:       -1,
	},
	3912: {
		Oid:          3912,
		ArrayTypeOid: 3913,
		Typname:      "daterange",
		Typlen:       -1,
	},
	3926: {
		Oid:          3926,
		ArrayTypeOid: 3927,
		Typname:      "int8range",
		Typlen:       -1,
	},
	2249: {
		Oid:          2249,
		ArrayTypeOid: 2287,
		Typname:      "record",
		Typlen:       -1,
	},
	2275: {
		Oid:          2275,
		ArrayTypeOid: 1263,
		Typname:      "cstring",
		Typlen:       -2,
	},
	13122: {
		Oid:          13122,
		ArrayTypeOid: 13121,
		Typname:      "cardinal_number",
		Typlen:       4,
	},
	13125: {
		Oid:          13125,
		ArrayTypeOid: 13124,
		Typname:      "character_data",
		Typlen:       -1,
	},
	13127: {
		Oid:          13127,
		ArrayTypeOid: 13126,
		Typname:      "sql_identifier",
		Typlen:       64,
	},
	13132: {
		Oid:          13132,
		ArrayTypeOid: 13131,
		Typname:      "time_stamp",
		Typlen:       8,
	},
	13134: {
		Oid:          13134,
		ArrayTypeOid: 13133,
		Typname:      "yes_or_no",
		Typlen:       -1,
	},
}
