package pglogrepl

import (
	"fmt"
)

// WalParser is a streaming wal parser of XLogData
// parser has internal state and result depends from right order of XLogData.
type WalParser struct {
	relations map[int32]RelationWalData
	lastRelation *RelationWalData
}

// NewWalParser ...
func NewWalParser() WalParser {
	return WalParser{relations: make(map[int32]RelationWalData)}
}

// Parse takes row XLogData and returns instance of WalData with value and data type
// Value are represented in Postgres' text format.
func (p *WalParser) Parse(xlog XLogData) (*WalData, error) {
	ty := Undefined
	payload := xlog.Data[1:]
	var wd Wal = nil
	var err error

	switch WalDataType(xlog.Data[0]) {
	case BeginWalType:
		wd, err = p.parseBeginWalData(payload)
		break
	case CommitWalType:
		wd, err = p.parseCommitWalData(payload)
		break
	case Insert:
		wd, err = p.parseInsertWalData(payload)
		break
	case Update:
		wd, err = p.parseUpdateWalData(payload)
		break
	case Delete:
		wd, err = p.parseDeleteWalData(payload)
		break
	case Truncate:
		wd, err = p.parseTruncateWalData(payload)
		break
	case Relation:
		relation, err := p.parseRelationWalData(payload)
		if err == nil {
			p.relations[relation.ID] = *relation
			wd = relation
		}
		break
	default:
		wd, err = NewUndefinedWalData(xlog.Data)
	}

	return &WalData{Type: ty, Value: wd}, err
}

func (p *WalParser) parseBeginWalData(data []byte) (*BeginWalData, error) {
	lsn := LSN(toInt64(data[:sizeOfInt64]))
	offset := sizeOfInt64

	timestamp := toInt64(data[offset:offset+sizeOfInt64])
	offset += sizeOfInt64

	xid := toInt32(data[offset:offset+sizeOfInt32])

	return &BeginWalData{
		Lsn: lsn,
		Timestamp: timestamp,
		XID: xid,
	}, nil
}

func (p *WalParser) parseRelationWalData(data []byte) (*RelationWalData, error) {
	offset := 0

	id := toInt32(data[:sizeOfInt32])
	offset += sizeOfInt32

	namespace, n := toString(data[offset:])
	offset += n

	relation, n := toString(data[offset:])
	offset += n

	relreplident := toInt8(data[offset : offset+sizeOfInt8])
	offset += sizeOfInt8

	nums := toInt16(data[offset : offset+sizeOfInt16])
	offset += sizeOfInt16

	columns := make([]RelationColumn, 0, nums)
	for i := int16(0); i < nums; i++ {
		flag := toBool(data[offset : offset+sizeOfBool])
		offset += sizeOfBool

		colname, n := toString(data[offset:])
		offset += n

		ty := toInt32(data[offset : offset+sizeOfInt32])
		offset += sizeOfInt32

		modifier := toInt32(data[offset : offset+sizeOfInt32])
		offset += sizeOfInt32

		pgty, isArray := GetPgTypeById(int(ty))
		columns = append(columns, RelationColumn{
			Flag:     flag,
			Name:     colname,
			Modifier: modifier,
			Type:     pgty,
			IsArray:  isArray,
		})
	}

	return &RelationWalData{ID: id,
		Namespace:    namespace,
		RelationName: relation,
		RelReplIdent: relreplident,
		ColumnsNum:   nums,
		Columns:      columns,
	}, nil
}

func (p *WalParser) parseCommitWalData(data []byte) (*CommitWalData, error) {
	offset := 0

	// skip first 8 byte, because it's currently unsused (must be 0)
	offset = sizeOfInt8

	lsn := toInt64(data[offset : offset+sizeOfInt64])
	offset += sizeOfInt64

	endLsn := toInt64(data[offset : offset+sizeOfInt64])
	offset += sizeOfInt64

	timestamp := toInt64(data[offset : offset+sizeOfInt64])
	offset += sizeOfInt64

	return &CommitWalData{LsnCommit: LSN(lsn),
		LsnTransaction: LSN(endLsn),
		Timestamp:      timestamp}, nil
}

func (p *WalParser) parseInsertWalData(data []byte) (*InsertWalData, error) {
	offset := 0
	insert := &InsertWalData{}

	insert.RelationId = toInt32(data[offset:offset+sizeOfInt32])
	offset += sizeOfInt32

	ident := data[offset]
	offset += sizeOfByte

	if ident != 'N' {
		return nil, fmt.Errorf("bad format for Insert, expected 'N' flag, but got %d (%s)", ident, string(ident))
	}

	rel, ok := p.relations[insert.RelationId]
	if !ok {
		return nil, fmt.Errorf("relation with ID=%d was not found", insert.RelationId)
	}

	insert.Relation = rel

	td, err := NewTupleData(data[offset:], rel)
	if err != nil {
		return nil, err
	}

	insert.Tuples = *td
	return insert, nil
}

func (p *WalParser) parseUpdateWalData(data []byte) (*UpdateWalData, error) {
	offset := 0
	update := &UpdateWalData{}

	update.RelationId = toInt32(data[offset:offset+sizeOfInt32])
	offset += sizeOfInt32

	ident := data[offset]
	offset += sizeOfByte

	rel, ok := p.relations[update.RelationId]
	if !ok {
		return nil, fmt.Errorf("relation with ID=%d was not found", update.RelationId)
	}

	update.Relation = rel

	switch ident {
	case 'K':
		return nil, fmt.Errorf("flag 'K' for Update is unsupported")
	case 'O':
		return nil, fmt.Errorf("flag 'O' for Update is unsupported")
	case 'N':
		td, err := NewTupleData(data[offset:], rel)
		if err != nil {
			return nil, err
		}

		update.Tuples = *td
		break
	default:
		return nil, fmt.Errorf("bad format to Update, expected 'K', 'O' or 'N' flag")
	}

	return update, nil
}

func (p *WalParser) parseDeleteWalData(data []byte) (*DeleteWalData, error) {
	offset := 0
	deleteWD := &DeleteWalData{}

	deleteWD.RelationId = toInt32(data[offset:offset+sizeOfInt32])
	offset += sizeOfInt32

	ident := data[offset]
	offset += sizeOfByte

	if ident != 'K' && ident != 'O' {
		return nil, fmt.Errorf("bad format for DELETE, expected 'K' or 'O' flag, but got %d (%s)",
			ident, string(ident))
	}

	rel, ok := p.relations[deleteWD.RelationId]
	if !ok {
		return nil, fmt.Errorf("relation with ID=%d was not found", deleteWD.RelationId)
	}

	deleteWD.Relation = rel

	td, err := NewTupleData(data[offset:], rel)
	if err != nil {
		return nil, err
	}

	deleteWD.Tuples = *td
	return deleteWD, nil
}

func (p *WalParser) parseTruncateWalData(data []byte) (*TruncateWalData, error) {
	offset := 0
	truncate := &TruncateWalData{}

	n := toInt32(data[offset:offset+sizeOfInt32])
	offset += sizeOfInt32

	flag := data[offset]
	truncate.IsCascade = flag == 1
	truncate.IsRestartIdentity = flag == 2
	offset += sizeOfInt8

	if len(data[offset:]) != int(n)*sizeOfInt32 {
		return nil, fmt.Errorf("bad format of Truncate")
	}

	truncate.Relations = make([]RelationWalData, 0, n)
	for i:=int32(0); i < n; i++ {
		id := toInt32(data[offset:offset+sizeOfInt32])
		offset += sizeOfInt32

		rel, ok := p.relations[id]
		if !ok {
			return nil, fmt.Errorf("relation with ID=%d was not found", id)
		}

		truncate.Relations = append(truncate.Relations, rel)
	}

	return truncate, nil
}