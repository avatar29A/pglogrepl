package pglogrepl

import (
	"fmt"
	"strings"
)

// WalDataType
// see: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
type WalDataType byte

const (
	BeginWalType  WalDataType = 'B'
	CommitWalType WalDataType = 'C'
	Relation      WalDataType = 'R'
	Insert        WalDataType = 'I'
	Update        WalDataType = 'U'
	Delete        WalDataType = 'D'
	Truncate      WalDataType = 'T'
	Undefined     WalDataType = '-'
)

const (
	sizeOfInt64 = 8
	sizeOfInt32 = 4
	sizeOfInt16 = 2
	sizeOfInt8 = 1
	sizeOfBool = 1
	sizeOfByte = 1
)

// Wal is a interface{} for WalData with advanced method String() to text representation.
type Wal interface {
	String() string
}

// WalData in a container for one of the possible XLogData record.
// some WalData are dependent from other. For example: Insert, Update, Delete depend on Relation.
type WalData struct {
	Type   WalDataType
	Value Wal
}

//
// BeginWalData corresponds the Begin command ('B')
type BeginWalData struct {
	Lsn LSN
	Timestamp int64
	XID int32
}

func (wd *BeginWalData) String() string {
	return fmt.Sprintf("BEGIN %d [TS: %d, LSN: %s]", wd.XID, wd.Timestamp, wd.Lsn.String())
}

//
// RelationWalData corresponds the Relation command ('R')
type RelationWalData struct {
	ID int32
	Namespace string
	RelationName string
	RelReplIdent int8
	ColumnsNum int16
	Columns []RelationColumn
}

type RelationColumn struct {
	Flag bool
	Modifier int32
	Name string
	Type PgType
	IsArray bool
}

func (wd *RelationWalData) FullName() string {
	return fmt.Sprintf("%s.%s", wd.Namespace, wd.RelationName)
}

func (wd *RelationWalData) String() string {
	columns := strings.Builder{}

	for _, c := range wd.Columns {
		arraySquares := ""
		if c.IsArray {
			arraySquares = "[]"
		}

		columns.WriteString(fmt.Sprintf("%s[%s%s] ", c.Name, c.Type.String(), arraySquares))
	}

	return fmt.Sprintf("RELATION: table %s.%s(%d): %s",
		wd.Namespace,
		wd.RelationName,
		wd.ID,
		columns.String())
}

//
// Commit
type CommitWalData struct {
	Flags int8
	LsnCommit LSN
	LsnTransaction LSN
	Timestamp int64
}

func (wd *CommitWalData) String() string {
	return fmt.Sprintf("COMMIT %s", wd.LsnCommit.String())
}

//
// Insert
type InsertWalData struct {
	RelationId int32
	Relation RelationWalData
	Tuples     TupleData
}

func (wd *InsertWalData) String() string {
	return "INSERT: " + wd.Relation.FullName() + " " + wd.Tuples.String()
}

//
// Update
type UpdateWalData struct {
	RelationId int32
	Relation RelationWalData
	Tuples TupleData
}

func (wd *UpdateWalData) String() string {
	return "UPDATE: " + wd.Relation.FullName() + " " + wd.Tuples.String()
}

//
// Delete
type DeleteWalData struct {
	RelationId int32
	Relation RelationWalData
	Tuples TupleData
}

func (wd *DeleteWalData) String() string {
	return "DELETE: " + wd.Relation.FullName() + " " + wd.Tuples.String()
}

//
// Truncate
type TruncateWalData struct {
	Relations         []RelationWalData
	IsCascade         bool
	IsRestartIdentity bool
}

func (wd *TruncateWalData) String() string {
	builder := strings.Builder{}
	builder.WriteString("TRUNCATE: ")
	builder.WriteString("[")

	for i, rel := range wd.Relations {
		if i > 0 {
			builder.WriteString(", ")
		}

		builder.WriteString(fmt.Sprintf("%s (%d)", rel.FullName(), rel.ID))
	}

	builder.WriteString("]")
	return builder.String()
}

//
// TupleData
type TupleData struct {
	Tuples []Tuple
}

type Tuple struct {
	RelCol RelationColumn
	Value []byte
	IsNull bool
	IsTOAST bool
}

func (t *Tuple) String() string {
	builder := strings.Builder{}

	builder.WriteString(fmt.Sprintf("%s[%s]:", t.RelCol.Name, t.RelCol.Type.Typname))
	if t.IsNull {
		builder.WriteString("NULL")
	} else if t.IsTOAST {
		builder.WriteString("<!TOAST>")
	} else {
		builder.WriteString(string(t.Value))
	}

	return builder.String()
}

// NewTupleData ...
func NewTupleData(bs []byte, rel RelationWalData) (*TupleData, error) {
	offset := 0
	td := &TupleData{}

	n := toInt16(bs[offset : offset+sizeOfInt16])
	offset += sizeOfInt16

	td.Tuples = make([]Tuple, 0, n)

	if n < rel.ColumnsNum {
		return nil, fmt.Errorf("mismatch schema with data. Expected %d columns, but got %d", n, rel.ColumnsNum)
	}

	for i := int16(0); i < n; i++ {
		ty := bs[offset]
		offset += sizeOfByte

		var tuple *Tuple = nil
		switch ty {
		case 'n':
			tuple = &Tuple{
				RelCol:      rel.Columns[i],
				Value:   nil,
				IsNull:  true,
			}
			break
		case 'u':
			tuple = &Tuple{
				RelCol:  rel.Columns[i],
				Value:   nil,
				IsTOAST: true,
			}
			break
		case 't':
			length := toInt32(bs[offset:offset+sizeOfInt32])
			offset += sizeOfInt32

			val := bs[offset:offset+int(length)]
			offset += int(length)

			tuple = &Tuple{
				RelCol: rel.Columns[i],
				Value: val,
			}
			break
		default:
			return nil, fmt.Errorf("bad TupleData format, expected 'n', 'u' or 't' flag")
		}

		td.Tuples = append(td.Tuples, *tuple)
	}

	return td, nil
}

func (td *TupleData) String() string {
	builder := strings.Builder{}
	for _, tuple := range td.Tuples {
		builder.WriteString(tuple.String() + " ")
	}

	return builder.String()
}

//
// UndefinedWalData presents an unrecognised command. Pseudo-code '-'.
type UndefinedWalData struct {
	data []byte
}

func NewUndefinedWalData(data []byte) (*UndefinedWalData, error) {
	return &UndefinedWalData{data: data}, nil
}

func (wd *UndefinedWalData) String() string {
	return fmt.Sprintf("UNDEFINED %s", string(wd.data))
}
