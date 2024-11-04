package main

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
)

type record map[string]interface{}
type GetTableRecordsResponse map[string]map[string][]record
type GetTableRecordResponse map[string]map[string]record
type PutTableRecord map[string]record
type ErrorResponse map[string]string

type Table struct {
	Name    string
	Columns []Column
}

type Column struct {
	Name            string
	Type            string
	IsNullable      bool
	IsPrimary       bool
	DefaultValue    interface{}
	IsAutoIncrement bool
}

func (c *Column) NewVar() interface{} {
	if c.Type == "int" {
		if c.IsNullable {
			return &sql.NullInt64{}
		}
		return new(int)
	} else {
		if c.IsNullable {
			return &sql.NullString{}
		}
		return new(string)
	}
}

func (c *Column) NewRecord(v interface{}) interface{} {
	switch rv := v.(type) {
	case *int:
		return *rv
	case *sql.NullInt64:
		if rv.Valid {
			return rv.Int64
		} else {
			return nil
		}
	case *string:
		return *rv
	case *sql.NullString:
		if rv.Valid {
			return rv.String
		} else {
			return nil
		}
	}
	return nil
}

func (col *Column) ValidateValue(val interface{}) error {
	_, ok1 := val.(string)
	fmt.Println(val)
	if val == nil && !col.IsNullable {
		return fmt.Errorf("field %s have invalid type", col.Name)
	}
	if ok1 && col.Type == "int" {
		return fmt.Errorf("field %s have invalid type", col.Name)
	}
	if !ok1 && col.Type == "string" && val != nil {
		return fmt.Errorf("field %s have invalid type", col.Name)
	}
	return nil
}

func (t *Table) GetRecordInterface() []interface{} {
	RecordData := make([]interface{}, len(t.Columns))
	for i := 0; i < len(t.Columns); i++ {
		RecordData[i] = t.Columns[i].NewVar()
	}
	return RecordData
}

type dbstruct struct {
	db     *sql.DB
	tables map[string]*Table
}

func (t *Table) GetPK() (string, error) {
	for _, col := range t.Columns {
		if col.IsPrimary {
			return col.Name, nil
		}
	}
	return "", fmt.Errorf("this table have not primary key")
}

func (t *Table) ValidatePostData(data map[string]interface{}) error {
	fmt.Println(data, "vps")
	for _, col := range t.Columns {
		val, ok := data[col.Name]
		if !ok {
			continue
		}
		if col.IsAutoIncrement && val != nil {
			return fmt.Errorf("field %s have invalid type", col.Name)
		}
		err := col.ValidateValue(val)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) FullFillPostData(data map[string]interface{}) ([]string, []interface{}, interface{}) {
	FCols := make([]string, 0)
	FVals := make([]interface{}, 0)
	var PK string
	for _, col := range t.Columns {
		val, ok := data[col.Name]
		if col.IsPrimary {
			PK = col.Name
		}
		if !ok {
			continue
		}
		FCols = append(FCols, fmt.Sprintf("%s = ?", col.Name))
		FVals = append(FVals, val)
	}
	return FCols, FVals, PK
}

func (t *Table) ValidateData(data map[string]interface{}) error {
	for _, col := range t.Columns {
		val, ok := data[col.Name]
		if !ok {
			data[col.Name] = col.NewVar()
			continue
		}
		err := col.ValidateValue(val)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) FilteringData(data map[string]interface{}) ([]string, []interface{}) {
	FCols := make([]string, 0)
	FVals := make([]interface{}, 0)
	for _, col := range t.Columns {
		val, ok := data[col.Name]
		if !ok && col.IsNullable {
			data[col.Name] = col.DefaultValue
		}
		if col.IsAutoIncrement {
			delete(data, col.Name)
			continue
		}
		FCols = append(FCols, col.Name)
		FVals = append(FVals, val)
	}
	return FCols, FVals
}

func (dbs *dbstruct) GetTables() (map[string]*Table, error) {
	names, err := GetTablesName(dbs.db)
	if err != nil {
		return nil, err
	}
	tablesmap := make(map[string]*Table, len(names))
	for _, val := range names {
		table := new(Table)
		table.Name = val
		columns, err := dbs.GetTablesColumn(table.Name)
		if err != nil {
			return nil, err
		}
		table.Columns = columns
		tablesmap[val] = table
	}
	return tablesmap, nil
}

func NewDbStruct(db *sql.DB) (*dbstruct, error) {
	dbs := &dbstruct{}
	dbs.db = db
	var err1 error
	dbs.tables, err1 = dbs.GetTables()
	return dbs, err1
}

func (db *dbstruct) GetTablesColumn(TableName string) ([]Column, error) {
	rows, err := db.db.Query("SHOW COLUMNS FROM " + TableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := make([]Column, 0)
	for rows.Next() {
		new_column := Column{}

		var (
			StrNull    string
			StrPrimary string
			StrIncr    string
		)
		rows.Scan(
			&new_column.Name,
			&new_column.Type,
			&StrNull,
			&StrPrimary,
			&new_column.DefaultValue,
			&StrIncr,
		)
		if StrNull == "YES" {
			new_column.IsNullable = true
		}
		if StrPrimary == "PRI" {
			new_column.IsPrimary = true
		}
		if StrIncr == "auto_increment" {
			new_column.IsAutoIncrement = true
		}
		if strings.Contains(new_column.Type, "int") {
			new_column.Type = "int"
		} else {
			new_column.Type = "string"
		}
		columns = append(columns, new_column)
	}
	fmt.Println(columns)
	return columns, nil
}

func GetTablesName(db *sql.DB) ([]string, error) {
	names := make([]string, 0)
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		err := rows.Scan(&name)
		if err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, nil
}

func (dbs *dbstruct) GetTableRow(t *Table, id int) (*GetTableRecordResponse, error) {
	PK, err := t.GetPK()
	if err != nil {
		return nil, err
	}
	row := dbs.db.QueryRow(fmt.Sprintf("SELECT * FROM %s WHERE %s= ?", t.Name, PK), id)
	RecordData := t.GetRecordInterface()
	m := make(record)
	err = row.Scan(RecordData...)
	if err != nil {
		return nil, err
	}
	for i, val := range RecordData {
		m[t.Columns[i].Name] = t.Columns[i].NewRecord(val)
	}
	row.Scan(RecordData...)
	return &GetTableRecordResponse{"response": {"record": m}}, nil
}

func (dbs *dbstruct) PutData(t *Table, data map[string]interface{}) (PutTableRecord, error) {
	err := t.ValidateData(data)
	if err != nil {
		return nil, err
	}
	FilteredCols, FilteredVals := t.FilteringData(data)
	q := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES(%s)",
		t.Name,
		strings.Join(FilteredCols, ","),
		strings.Join(strings.Split(strings.Repeat("?", len(FilteredVals)), ""), ", "),
	)
	r, err := dbs.db.Exec(q, FilteredVals...)
	if err != nil {
		return nil, err
	}
	id, err := r.LastInsertId()
	if err != nil {
		return nil, err
	}
	PK, err := t.GetPK()
	if err != nil {
		return nil, err
	}
	return PutTableRecord{"response": record{PK: id}}, nil

}
func (dbs *dbstruct) DeleteData(t *Table, id int) (PutTableRecord, error) {
	rows, err := dbs.db.Exec("DELETE FROM "+t.Name+" WHERE id = ?", id)
	if err != nil {
		return nil, err
	}
	deleted, err := rows.RowsAffected()
	if err != nil {
		return nil, err
	}
	return PutTableRecord{"response": record{"deleted": deleted}}, nil
}

func (dbs *dbstruct) PostData(t *Table, data map[string]interface{}, id int) (PutTableRecord, error) {
	err := t.ValidatePostData(data)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	FilteredCols, FilteredVals, PK := t.FullFillPostData(data)
	q := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s = ?",
		t.Name,
		strings.Join(FilteredCols, ", "),
		PK,
	)
	FilteredVals = append(FilteredVals, id)
	r, err := dbs.db.Exec(q, FilteredVals...)
	if err != nil {
		return nil, err
	}

	updated, err := r.RowsAffected()
	if err != nil {
		return nil, err
	}
	return PutTableRecord{"response": record{"updated": updated}}, nil

}

func (dbs *dbstruct) GetTableData(t *Table, limit int, offset int) (*GetTableRecordsResponse, error) {
	RecordData := t.GetRecordInterface()
	rows, err := dbs.db.Query("SELECT * FROM "+t.Name+" LIMIT ? OFFSET ?", limit, offset)
	if err != nil {
		return nil, err
	}

	records := []record{}
	for rows.Next() {
		m := make(record)
		rows.Scan(RecordData...)
		for i, val := range RecordData {
			m[t.Columns[i].Name] = t.Columns[i].NewRecord(val)
		}
		records = append(records, m)
	}
	rows.Close()
	return &GetTableRecordsResponse{"response": {"records": records}}, nil
}

func (dbs *dbstruct) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" {
		names, err := GetTablesName(dbs.db)
		if err != nil {
		}
		JsTables, _ := json.Marshal(record{"response": record{"tables": names}})
		w.WriteHeader(200)
		w.Write(JsTables)
		return
	}
	urlParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	table, ok := dbs.tables[urlParts[0]]
	if !ok {
		w.WriteHeader(404)
		JsErr, _ := json.Marshal(ErrorResponse{"error": "unknown table"})
		w.Write(JsErr)
		return
	}
	switch len(urlParts) {
	case 1:
		switch r.Method {
		case "GET":
			q := r.URL.Query()
			limit, err1 := strconv.Atoi(q.Get("limit"))
			offset, err2 := strconv.Atoi(q.Get("offset"))
			if err1 != nil {
				limit = 5
			}
			if err2 != nil {
				offset = 0
			}
			res, err := dbs.GetTableData(dbs.tables[table.Name], limit, offset)
			if err != nil {
			}
			brecs, _ := json.Marshal(res)
			fmt.Println("))")
			w.WriteHeader(200)
			w.Write(brecs)
			return
		case "PUT":
			body, err := ioutil.ReadAll(r.Body)
			if err != nil {

			}
			data := make(map[string]interface{})
			err = json.Unmarshal(body, &data)
			if err != nil {
			}
			PutData, _ := dbs.PutData(dbs.tables[table.Name], data)
			res, err := json.Marshal(PutData)
			if err != nil {
				JsErr, _ := json.Marshal(record{"error": err.Error()})
				w.Write(JsErr)
				return
			}
			w.Write(res)
		}

	case 2:
		b, _ := json.Marshal(map[string]int{"id": 1})
		m := make(map[string]interface{})
		json.Unmarshal(b, &m)
		fmt.Println(m)
		switch r.Method {
		case "GET":
			id, err := strconv.Atoi(urlParts[1])
			if err != nil {
				err, _ := json.Marshal(record{"error": "id must be integer"})
				w.Write(err)
				return
			}
			res, err := dbs.GetTableRow(dbs.tables[table.Name], id)
			if err != nil {
				err, _ := json.Marshal(record{"error": "record not found"})
				w.WriteHeader(404)
				w.Write(err)
				return
			}
			brecs, _ := json.Marshal(res)
			w.Write(brecs)
			return
		case "POST":
			id, err := strconv.Atoi(urlParts[1])
			if err != nil {
				err, _ := json.Marshal(record{"error": "id must be integer"})
				w.Write(err)
				return
			}
			_, err = dbs.GetTableRow(dbs.tables[table.Name], id)
			if err != nil {
				err, _ := json.Marshal(record{"error": "record not found"})
				w.WriteHeader(404)
				w.Write(err)
				return
			}
			body, err := ioutil.ReadAll(r.Body)
			if err != nil {

			}
			data := make(map[string]interface{})
			err = json.Unmarshal(body, &data)
			if err != nil {
				JsErr, _ := json.Marshal(record{"error": err.Error()})
				w.Write(JsErr)
				return
			}
			PutData, err := dbs.PostData(dbs.tables[table.Name], data, id)
			if err != nil {
				w.WriteHeader(400)
				ErrVal, _ := json.Marshal(ErrorResponse{"error": err.Error()})
				w.Write(ErrVal)
				return
			}
			JsRes, _ := json.Marshal(PutData)
			if JsRes != nil {
			}
			w.Write(JsRes)
		case "DELETE":
			id, err := strconv.Atoi(urlParts[1])
			if err != nil {
				err, _ := json.Marshal(record{"error": "id must be integer"})
				w.Write(err)
				return
			}
			res, err := dbs.DeleteData(dbs.tables[table.Name], id)
			if err != nil {
				JsErr, _ := json.Marshal(record{"error": err.Error()})
				w.Write(JsErr)
				return
			}
			JsRes, err := json.Marshal(res)
			if err != nil {
				JsErr, _ := json.Marshal(record{"error": err.Error()})
				w.Write(JsErr)
				return
			}
			w.Write(JsRes)
		}
	}
}

func NewDbExplorer(db *sql.DB) (*dbstruct, error) {
	dbs, err := NewDbStruct(db)
	if err != nil {
		return nil, err
	}
	return dbs, nil
}
