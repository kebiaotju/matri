package mysqldb

import (
	"errors"
	"fmt"
	"strconv"
)

const (
	errNilRow = "Row: nil row"
	errNoKey  = "Row: no key"
	errFmt    = "Row: invalied fmt"
)

//Row err
var (
	ErrNilRow = errors.New(errNilRow)
	ErrNoKey  = errors.New(errNoKey)
	ErrFmt    = errors.New(errFmt)
)

type row map[string][]byte

//Row is mysql query row.
//Row can fetch col elemnet by special type & catch error, Err is the last error range(ErrNilRow, ErrNoKey, ErrFmt).
//if UseDetailedErr is true, when error has occured, it will also create a detail error to locate error.
//if Strict is true, when error has occured, other call will not work except Reset & ClearErr util call Reset or ClearErr.
//if Strict is false, program will panic when Row not Reset but call any col fetch function(such as String, Bool, Int32...).
type Row struct {
	Err            error
	DetailedErr    error
	UseDetailedErr bool
	Strict         bool

	innerRow row
}

//NewRow create a default Row and return it`s pointer
func NewRow() *Row {
	return &Row{
		Err:            nil,
		DetailedErr:    nil,
		UseDetailedErr: false,
		Strict:         false,
		innerRow:       nil,
	}
}

//CreateRow return a special Row init with args
func CreateRow(args map[string][]byte) *Row {
	r := NewRow()
	r.Reset(args)
	return r
}

//Reset Row with args
func (r *Row) Reset(args map[string][]byte) {
	r.ClearErr()
	r.innerRow = (row)(args)
}

//ClearErr clear all error
func (r *Row) ClearErr() {
	r.Err = nil
	//r.DetailedErr = nil
}

//ClearErrIf clear error when r.Err == err
func (r *Row) ClearErrIf(err error) {
	if r.Err == err {
		r.ClearErr()
	}
}

//Bytes return col by []byte
func (r *Row) Bytes(key string) (ret []byte) {
	if r.Strict && r.Err != nil {
		return
	}
	ret, _ = r.checkKeyRaw(key)
	return
}

//String return col by string
func (r *Row) String(key string) (ret string) {
	if r.Strict && r.Err != nil {
		return
	}
	ret, _ = r.checkKey(key)
	return
}

//Bool return col by bool
func (r *Row) Bool(key string) (ret bool) {
	r.fmtV(key, func(value string) { ret = r.getBool(value) })
	return
}

//Int32 return col by int32
func (r *Row) Int32(key string) (ret int32) {
	return int32(r.Int64(key))
}

//Int64 return col by int64
func (r *Row) Int64(key string) (ret int64) {
	r.fmtV(key, func(value string) { ret = r.getInt64(value) })
	return
}

//Uint32 return col by uint32
func (r *Row) Uint32(key string) (ret uint32) {
	return uint32(r.Uint64(key))
}

//Uint64 return col by uint64
func (r *Row) Uint64(key string) (ret uint64) {
	r.fmtV(key, func(value string) { ret = r.getUint64(value) })
	return
}

//Float32 return col by float32
func (r *Row) Float32(key string) (ret float32) {
	return float32(r.Float64(key))
}

//Float64 return col by float64
func (r *Row) Float64(key string) (ret float64) {
	r.fmtV(key, func(value string) { ret = r.getFloat64(value) })
	return
}

///////////////////////////////////

func (r *Row) checkKeyRaw(key string) ([]byte, bool) {
	if r.Strict && r.Err != nil {
		return nil, false
	}
	if r.innerRow == nil {
		r.Err = ErrNilRow
		if r.UseDetailedErr {
			r.DetailedErr = r.Err
		}
		return nil, false
	}
	if row, ok := r.innerRow[key]; ok {
		return row, true
	}
	r.Err = ErrNoKey
	if r.UseDetailedErr {
		r.DetailedErr = fmt.Errorf(errNoKey+" %v", key)
	}
	return nil, false
}

func (r *Row) checkKey(key string) (string, bool) {
	value, ok := r.checkKeyRaw(key)
	return string(value), ok
}

func (r *Row) checkFmtErr(err error) {
	if err != nil {
		r.Err = ErrFmt
		r.DetailedErr = err
	}
}

func (r *Row) buildFmtDetailedErr(key string) {
	if r.UseDetailedErr && r.Err != nil {
		r.DetailedErr = fmt.Errorf(r.Err.Error()+" fmt key[%v], [%v]", key, r.DetailedErr)
	}
}

func (r *Row) fmtV(key string, f func(string)) {
	if r.Err != nil {
		return
	}
	if value, ok := r.checkKey(key); ok {
		f(value)
		r.buildFmtDetailedErr(key)
	}
	return
}

func (r *Row) getBool(value string) (ret bool) {
	var err error
	ret, err = strconv.ParseBool(value)
	r.checkFmtErr(err)
	return
}

func (r *Row) getInt64(value string) (ret int64) {
	var err error
	ret, err = strconv.ParseInt(value, 10, 64)
	r.checkFmtErr(err)
	return
}

func (r *Row) getUint64(value string) (ret uint64) {
	var err error
	ret, err = strconv.ParseUint(value, 10, 64)
	r.checkFmtErr(err)
	return
}

func (r *Row) getFloat64(value string) (ret float64) {
	var err error
	ret, err = strconv.ParseFloat(value, 64)
	r.checkFmtErr(err)
	return
}
