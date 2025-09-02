package orm

import (
	"database/sql"
	"fmt"
	"strings"
)

const (
	// queryTypeSELECT represents a SELECT query.
	queryTypeSELECT = iota + 1
	// queryTypeUPDATE represents an UPDATE query.
	queryTypeUPDATE
	// queryTypeDelete represents a DELETE query.
	queryTypeDelete
)

// QueryBuilder provides a fluent API for building and executing SQL queries.
type QueryBuilder[OUTPUT any] struct {
	queryType int
	schema    *schema

	// Common query components
	whereClause          *whereClause
	tableName            string
	placeholderGenerator func(n int) []string

	// SELECT query components
	orderByClause *orderByClause
	groupByClause *GroupBy
	selected      *selected
	subQuery      *struct {
		query                string
		args                 []any
		placeholderGenerator func(n int) []string
	}
	joins        []*Join
	limitClause  *Limit
	offsetClause *Offset

	// UPDATE query components
	setValues [][2]any

	// Execution components
	db  *sql.DB
	err error
}

// Execute executes the UPDATE or DELETE query built by the QueryBuilder.
// It returns an error if the query type is SELECT.
func (qb *QueryBuilder[OUTPUT]) Execute() (sql.Result, error) {
	if qb.err != nil {
		return nil, qb.err
	}

	if qb.queryType == queryTypeSELECT {
		return nil, fmt.Errorf("cannot execute a SELECT query with this method")
	}

	query, args, err := qb.ToSql()
	if err != nil {
		return nil, err
	}

	return qb.schema.getConnection().exec(query, args...)
}

// Get executes the query and scans the first result into the OUTPUT type.
func (qb *QueryBuilder[OUTPUT]) Get() (OUTPUT, error) {
	if qb.err != nil {
		return *new(OUTPUT), qb.err
	}

	queryString, args, err := qb.ToSql()
	if err != nil {
		return *new(OUTPUT), err
	}

	rows, err := qb.schema.getConnection().query(queryString, args...)
	if err != nil {
		return *new(OUTPUT), err
	}

	var output OUTPUT
	err = newBinder(qb.schema).bind(rows, &output)
	if err != nil {
		return *new(OUTPUT), err
	}

	return output, nil
}

// All executes the SELECT query and scans all results into a slice of the OUTPUT type.
func (qb *QueryBuilder[OUTPUT]) All() ([]OUTPUT, error) {
	if qb.err != nil {
		return nil, qb.err
	}

	qb.SetSelect()
	queryString, args, err := qb.ToSql()
	if err != nil {
		return nil, err
	}

	rows, err := qb.schema.getConnection().query(queryString, args...)
	if err != nil {
		return nil, err
	}

	var output []OUTPUT
	err = newBinder(qb.schema).bind(rows, &output)
	if err != nil {
		return nil, err
	}

	return output, nil
}

// Delete builds and executes a DELETE query.
func (qb *QueryBuilder[OUTPUT]) Delete() (rowsAffected int64, err error) {
	if qb.err != nil {
		return 0, qb.err
	}

	qb.SetDelete()
	res, err := qb.Execute()
	if err != nil {
		return 0, qb.err
	}

	return res.RowsAffected()
}

// Update builds and executes an UPDATE query.
func (qb *QueryBuilder[OUTPUT]) Update() (rowsAffected int64, err error) {
	if qb.err != nil {
		return 0, qb.err
	}

	qb.SetUpdate()
	res, err := qb.Execute()
	if err != nil {
		return 0, qb.err
	}

	return res.RowsAffected()
}

func copyQueryBuilder[T1 any, T2 any](q *QueryBuilder[T1], q2 *QueryBuilder[T2]) {
	q2.db = q.db
	q2.err = q.err
	q2.groupByClause = q.groupByClause
	q2.joins = q.joins
	q2.limitClause = q.limitClause
	q2.offsetClause = q.offsetClause
	q2.orderByClause = q.orderByClause
	q2.placeholderGenerator = q.placeholderGenerator
	q2.schema = q.schema
	q2.selected = q.selected
	q2.setValues = q.setValues

	q2.subQuery = q.subQuery
	q2.tableName = q.tableName
	q2.queryType = q.queryType
	q2.whereClause = q.whereClause
}

// Count executes a COUNT(id) query and returns a new QueryBuilder for the result.
func (qb *QueryBuilder[OUTPUT]) Count() *QueryBuilder[int] {
	qb.selected = &selected{Columns: []string{"COUNT(id)"}}
	qb.SetSelect()
	qCount := NewQueryBuilder[int](qb.schema)

	copyQueryBuilder(qb, qCount)

	return qCount
}

// First retrieves the first record, ordered by the primary key in ascending order.
func (qb *QueryBuilder[OUTPUT]) First() *QueryBuilder[OUTPUT] {
	qb.OrderBy(qb.schema.pkName(), ASC).Limit(1)

	return qb
}

// Latest retrieves the latest record, ordered by the primary key in descending order.
func (qb *QueryBuilder[OUTPUT]) Latest() *QueryBuilder[OUTPUT] {
	qb.OrderBy(qb.schema.pkName(), DESC).Limit(1)

	return qb
}

// WherePK adds a WHERE clause to filter by the primary key.
func (qb *QueryBuilder[OUTPUT]) WherePK(value any) *QueryBuilder[OUTPUT] {
	return qb.Where(qb.schema.pkName(), value)
}

func (qb *QueryBuilder[OUTPUT]) toSqlDelete() (string, []any, error) {
	base := fmt.Sprintf("DELETE FROM %s", qb.tableName)

	var args []any
	if qb.whereClause != nil {
		qb.whereClause.PlaceHolderGenerator = qb.placeholderGenerator
		where, whereArgs, err := qb.whereClause.ToSql()
		if err != nil {
			return "", nil, err
		}
		base += " WHERE " + where
		args = append(args, whereArgs...)
	}

	return base, args, nil
}

func pop(phs *[]string) string {
	top := (*phs)[len(*phs)-1]
	*phs = (*phs)[:len(*phs)-1]

	return top
}

func (qb *QueryBuilder[OUTPUT]) kvString() string {
	phs := qb.placeholderGenerator(len(qb.setValues))
	sets := make([]string, 0, len(qb.setValues))

	for _, pair := range qb.setValues {
		sets = append(sets, fmt.Sprintf("%s=%s", pair[0], pop(&phs)))
	}

	return strings.Join(sets, ",")
}

func (qb *QueryBuilder[OUTPUT]) args() []any {
	values := make([]any, 0, len(qb.setValues))
	for _, pair := range qb.setValues {
		values = append(values, pair[1])
	}

	return values
}

func (qb *QueryBuilder[OUTPUT]) toSqlUpdate() (string, []any, error) {
	if qb.tableName == "" {
		return "", nil, fmt.Errorf("table cannot be empty")
	}

	base := fmt.Sprintf("UPDATE %s SET %s", qb.tableName, qb.kvString())
	args := qb.args()
	if qb.whereClause != nil {
		qb.whereClause.PlaceHolderGenerator = qb.placeholderGenerator
		where, whereArgs, err := qb.whereClause.ToSql()
		if err != nil {
			return "", nil, err
		}
		args = append(args, whereArgs...)
		base += " WHERE " + where
	}

	return base, args, nil
}

func (qb *QueryBuilder[OUTPUT]) toSqlSelect() (string, []any, error) {
	if qb.err != nil {
		return "", nil, qb.err
	}

	base := "SELECT"

	var args []any
	// select
	if qb.selected == nil {
		qb.selected = &selected{
			Columns: []string{"*"},
		}
	}

	base += " " + qb.selected.String()
	// from
	if qb.tableName == "" && qb.subQuery == nil {
		return "", nil, fmt.Errorf("Table name cannot be empty")
	} else if qb.tableName != "" && qb.subQuery != nil {
		return "", nil, fmt.Errorf("cannot have both Table and subquery")
	}

	if qb.tableName != "" {
		base += " " + "FROM " + qb.tableName
	}

	if qb.subQuery != nil {
		qb.subQuery.placeholderGenerator = qb.placeholderGenerator
		base += " " + "FROM (" + qb.subQuery.query + " )"
		args = append(args, qb.subQuery.args...)
	}

	// Joins
	if qb.joins != nil {
		for _, join := range qb.joins {
			base += " " + join.String()
		}
	}

	// whereClause
	if qb.whereClause != nil {
		qb.whereClause.PlaceHolderGenerator = qb.placeholderGenerator
		where, whereArgs, err := qb.whereClause.ToSql()
		if err != nil {
			return "", nil, err
		}
		base += " WHERE " + where
		args = append(args, whereArgs...)
	}

	// orderByClause
	if qb.orderByClause != nil {
		base += " " + qb.orderByClause.String()
	}

	// GroupBy
	if qb.groupByClause != nil {
		base += " " + qb.groupByClause.String()
	}

	// Limit
	if qb.limitClause != nil {
		base += " " + qb.limitClause.String()
	}

	// Offset
	if qb.offsetClause != nil {
		base += " " + qb.offsetClause.String()
	}

	return base, args, nil
}

// ToSql builds the SQL query string and its arguments.
func (qb *QueryBuilder[OUTPUT]) ToSql() (string, []any, error) {
	if qb.err != nil {
		return "", nil, qb.err
	}

	switch qb.queryType {
	case queryTypeSELECT:
		return qb.toSqlSelect()
	case queryTypeDelete:
		return qb.toSqlDelete()
	case queryTypeUPDATE:
		return qb.toSqlUpdate()
	default:
		return "", nil, fmt.Errorf("unknown query type")
	}
}

const (
	ASC  string = "ASC"
	DESC string = "DESC"
)

type orderByClause struct {
	Columns [][2]string
}

func (o orderByClause) String() string {
	tuples := make([]string, 0, len(o.Columns))

	for _, pair := range o.Columns {
		tuples = append(tuples, fmt.Sprintf("%s %s", pair[0], pair[1]))
	}

	return fmt.Sprintf("ORDER BY %s", strings.Join(tuples, ","))
}

type GroupBy struct {
	Columns []string
}

func (g GroupBy) String() string {
	return fmt.Sprintf("GROUP BY %s", strings.Join(g.Columns, ","))
}

type joinType string

const (
	JoinTypeInner = "INNER"
	JoinTypeLeft  = "LEFT"
	JoinTypeRight = "RIGHT"
	JoinTypeFull  = "FULL OUTER"
	JoinTypeSelf  = "SELF"
)

type JoinOn struct {
	Lhs string
	Rhs string
}

func (j JoinOn) String() string {
	return fmt.Sprintf("%s = %s", j.Lhs, j.Rhs)
}

type Join struct {
	Type  joinType
	Table string
	On    JoinOn
}

func (j Join) String() string {
	return fmt.Sprintf("%s JOIN %s ON %s", j.Type, j.Table, j.On.String())
}

type Limit struct {
	N int
}

func (l Limit) String() string {
	return fmt.Sprintf("LIMIT %d", l.N)
}

type Offset struct {
	N int
}

func (o Offset) String() string {
	return fmt.Sprintf("OFFSET %d", o.N)
}

type selected struct {
	Columns []string
}

func (s selected) String() string {
	return strings.Join(s.Columns, ",")
}

// OrderBy adds an ORDER BY clause to the query.
func (qb *QueryBuilder[OUTPUT]) OrderBy(column string, order string) *QueryBuilder[OUTPUT] {
	qb.SetSelect()
	if qb.orderByClause == nil {
		qb.orderByClause = &orderByClause{}
	}
	qb.orderByClause.Columns = append(qb.orderByClause.Columns, [2]string{column, order})
	return qb
}

// LeftJoin adds a LEFT JOIN clause to the query.
func (qb *QueryBuilder[OUTPUT]) LeftJoin(table string, onLhs string, onRhs string) *QueryBuilder[OUTPUT] {
	qb.SetSelect()
	qb.joins = append(qb.joins, &Join{
		Type:  JoinTypeLeft,
		Table: table,
		On: JoinOn{
			Lhs: onLhs,
			Rhs: onRhs,
		},
	})

	return qb
}

// RightJoin adds a RIGHT JOIN clause to the query.
func (qb *QueryBuilder[OUTPUT]) RightJoin(table string, onLhs string, onRhs string) *QueryBuilder[OUTPUT] {
	qb.SetSelect()
	qb.joins = append(qb.joins, &Join{
		Type:  JoinTypeRight,
		Table: table,
		On: JoinOn{
			Lhs: onLhs,
			Rhs: onRhs,
		},
	})

	return qb
}

// InnerJoin adds an INNER JOIN clause to the query.
func (qb *QueryBuilder[OUTPUT]) InnerJoin(table string, onLhs string, onRhs string) *QueryBuilder[OUTPUT] {
	qb.SetSelect()
	qb.joins = append(qb.joins, &Join{
		Type:  JoinTypeInner,
		Table: table,
		On: JoinOn{
			Lhs: onLhs,
			Rhs: onRhs,
		},
	})

	return qb
}

// Join adds an INNER JOIN clause to the query.
func (qb *QueryBuilder[OUTPUT]) Join(table string, onLhs string, onRhs string) *QueryBuilder[OUTPUT] {
	return qb.InnerJoin(table, onLhs, onRhs)
}

// FullOuterJoin adds a FULL OUTER JOIN clause to the query.
func (qb *QueryBuilder[OUTPUT]) FullOuterJoin(table string, onLhs string, onRhs string) *QueryBuilder[OUTPUT] {
	qb.SetSelect()
	qb.joins = append(qb.joins, &Join{
		Type:  JoinTypeFull,
		Table: table,
		On: JoinOn{
			Lhs: onLhs,
			Rhs: onRhs,
		},
	})

	return qb
}

// Where adds a WHERE clause to the query.
func (qb *QueryBuilder[OUTPUT]) Where(parts ...any) *QueryBuilder[OUTPUT] {
	if qb.whereClause != nil {
		return qb.addWhere("AND", parts...)
	}

	if len(parts) == 1 {
		if r, isRaw := parts[0].(*raw); isRaw {
			qb.whereClause = &whereClause{raw: r.sql, args: r.args, PlaceHolderGenerator: qb.placeholderGenerator}
			return qb
		} else {
			qb.err = fmt.Errorf("when you have one argument passed to where, it should be *raw")
			return qb
		}
	} else if len(parts) == 2 {
		if !strings.Contains(parts[0].(string), " ") {
			// Equal mode
			qb.whereClause = &whereClause{cond: cond{Lhs: parts[0].(string), Op: Eq, Rhs: parts[1]}, PlaceHolderGenerator: qb.placeholderGenerator}
		}

		return qb
	} else if len(parts) == 3 {
		// operator mode
		qb.whereClause = &whereClause{cond: cond{Lhs: parts[0].(string), Op: binaryOp(parts[1].(string)), Rhs: parts[2]}, PlaceHolderGenerator: qb.placeholderGenerator}

		return qb
	} else if len(parts) > 3 && parts[1].(string) == "IN" {
		qb.whereClause = &whereClause{cond: cond{Lhs: parts[0].(string), Op: binaryOp(parts[1].(string)), Rhs: parts[2:]}, PlaceHolderGenerator: qb.placeholderGenerator}

		return qb
	} else {
		qb.err = fmt.Errorf("wrong number of arguments passed to Where clause")

		return qb
	}
}

type binaryOp string

const (
	Eq      = "="
	GT      = ">"
	LT      = "<"
	GE      = ">="
	LE      = "<="
	NE      = "!="
	Between = "BETWEEN"
	Like    = "LIKE"
	In      = "IN"
)

type cond struct {
	PlaceHolderGenerator func(n int) []string

	Lhs string
	Op  binaryOp
	Rhs any
}

func (b cond) ToSql() (string, []any, error) {
	var phs []string

	if b.Op == In {
		rhs, isInterfaceSlice := b.Rhs.([]any)
		if isInterfaceSlice {
			phs = b.PlaceHolderGenerator(len(rhs))

			return fmt.Sprintf("%s IN (%s)", b.Lhs, strings.Join(phs, ",")), rhs, nil
		} else if rawThing, isRaw := b.Rhs.(*raw); isRaw {
			return fmt.Sprintf("%s IN (%s)", b.Lhs, rawThing.sql), rawThing.args, nil
		} else {
			return "", nil, fmt.Errorf("right side of Cond when operator is IN should be either a any slice or *raw")
		}
	} else {
		phs = b.PlaceHolderGenerator(1)
		return fmt.Sprintf("%s %s %s", b.Lhs, b.Op, pop(&phs)), []any{b.Rhs}, nil
	}
}

const (
	nextType_AND = "AND"
	nextType_OR  = "OR"
)

type whereClause struct {
	PlaceHolderGenerator func(n int) []string
	nextTyp              string
	next                 *whereClause
	cond
	raw  string
	args []any
}

func (w whereClause) ToSql() (string, []any, error) {
	var (
		base string
		args []any
		err  error
	)

	if w.raw != "" {
		base = w.raw
		args = w.args
	} else {
		w.cond.PlaceHolderGenerator = w.PlaceHolderGenerator
		base, args, err = w.cond.ToSql()

		if err != nil {
			return "", nil, err
		}
	}

	if w.next == nil {
		return base, args, nil
	}

	if w.next != nil {
		next, nextArgs, err := w.next.ToSql()
		if err != nil {
			return "", nil, err
		}

		base += " " + w.nextTyp + " " + next
		args = append(args, nextArgs...)

		return base, args, nil
	}

	return base, args, nil
}

// WhereIn adds a WHERE IN clause to the query.
func (qb *QueryBuilder[OUTPUT]) WhereIn(column string, values ...any) *QueryBuilder[OUTPUT] {
	return qb.Where(append([]any{column, In}, values...)...)
}

// AndWhere adds an AND WHERE clause to the query.
func (qb *QueryBuilder[OUTPUT]) AndWhere(parts ...any) *QueryBuilder[OUTPUT] {
	return qb.addWhere(nextType_AND, parts...)
}

// OrWhere adds an OR WHERE clause to the query.
func (qb *QueryBuilder[OUTPUT]) OrWhere(parts ...any) *QueryBuilder[OUTPUT] {
	return qb.addWhere(nextType_OR, parts...)
}

func (qb *QueryBuilder[OUTPUT]) addWhere(typ string, parts ...any) *QueryBuilder[OUTPUT] {
	w := qb.whereClause

	for {
		if w == nil {
			break
		} else if w.next == nil {
			w.next = &whereClause{PlaceHolderGenerator: qb.placeholderGenerator}
			w.nextTyp = typ
			w = w.next
			break
		} else {
			w = w.next
		}
	}

	if w == nil {
		w = &whereClause{PlaceHolderGenerator: qb.placeholderGenerator}
	}

	if len(parts) == 1 {
		w.raw = parts[0].(*raw).sql
		w.args = parts[0].(*raw).args
		return qb
	} else if len(parts) == 2 {
		// Equal mode
		w.cond = cond{Lhs: parts[0].(string), Op: Eq, Rhs: parts[1]}
		return qb
	} else if len(parts) == 3 {
		// operator mode
		w.cond = cond{Lhs: parts[0].(string), Op: binaryOp(parts[1].(string)), Rhs: parts[2]}
		return qb
	} else {
		panic("wrong number of arguments passed to Where")
	}
}

// Offset adds an OFFSET clause to the query.
func (qb *QueryBuilder[OUTPUT]) Offset(n int) *QueryBuilder[OUTPUT] {
	qb.SetSelect()
	qb.offsetClause = &Offset{N: n}

	return qb
}

// Limit adds a LIMIT clause to the query.
func (qb *QueryBuilder[OUTPUT]) Limit(n int) *QueryBuilder[OUTPUT] {
	qb.SetSelect()
	qb.limitClause = &Limit{N: n}

	return qb
}

// Table sets the table for the query.
func (qb *QueryBuilder[OUTPUT]) Table(t string) *QueryBuilder[OUTPUT] {
	qb.tableName = t

	return qb
}

// SetSelect sets the query type to SELECT.
func (qb *QueryBuilder[OUTPUT]) SetSelect() *QueryBuilder[OUTPUT] {
	qb.queryType = queryTypeSELECT

	return qb
}

// GroupBy adds a GROUP BY clause to the query.
func (qb *QueryBuilder[OUTPUT]) GroupBy(columns ...string) *QueryBuilder[OUTPUT] {
	qb.SetSelect()
	if qb.groupByClause == nil {
		qb.groupByClause = &GroupBy{}
	}

	qb.groupByClause.Columns = append(qb.groupByClause.Columns, columns...)

	return qb
}

// Select adds columns to the SELECT clause of the query.
func (qb *QueryBuilder[OUTPUT]) Select(columns ...string) *QueryBuilder[OUTPUT] {
	qb.SetSelect()

	if qb.selected == nil {
		qb.selected = &selected{}
	}

	qb.selected.Columns = append(qb.selected.Columns, columns...)
	return qb
}

// FromQuery sets a subquery for the FROM clause.
func (qb *QueryBuilder[OUTPUT]) FromQuery(subQuery *QueryBuilder[OUTPUT]) *QueryBuilder[OUTPUT] {
	qb.SetSelect()

	subQuery.SetSelect()
	subQuery.placeholderGenerator = qb.placeholderGenerator
	subQueryString, args, err := subQuery.ToSql()

	qb.err = err
	qb.subQuery = &struct {
		query                string
		args                 []any
		placeholderGenerator func(n int) []string
	}{
		subQueryString, args, qb.placeholderGenerator,
	}

	return qb
}

// SetUpdate sets the query type to UPDATE.
func (qb *QueryBuilder[OUTPUT]) SetUpdate() *QueryBuilder[OUTPUT] {
	qb.queryType = queryTypeUPDATE

	return qb
}

// Set adds SET clauses to an UPDATE query.
func (qb *QueryBuilder[OUTPUT]) Set(keyValues ...any) *QueryBuilder[OUTPUT] {
	if len(keyValues)%2 != 0 {
		qb.err = fmt.Errorf("when using Set, passed argument count should be even: %w", qb.err)
		return qb
	}

	qb.SetUpdate()

	for i := range keyValues {
		if i != 0 && i%2 == 1 {
			qb.setValues = append(qb.setValues, [2]any{keyValues[i-1], keyValues[i]})
		}
	}

	return qb
}

// SetDialect sets the dialect for the query.
func (qb *QueryBuilder[OUTPUT]) SetDialect(dialect *Dialect) *QueryBuilder[OUTPUT] {
	qb.placeholderGenerator = dialect.PlaceHolderGenerator

	return qb
}

// SetDelete sets the query type to DELETE.
func (qb *QueryBuilder[OUTPUT]) SetDelete() *QueryBuilder[OUTPUT] {
	qb.queryType = queryTypeDelete

	return qb
}

type raw struct {
	sql  string
	args []any
}

// Raw creates a Raw sql query chunk that you can add to several components of QueryBuilder like
// Wheres.
func Raw(sql string, args ...any) *raw {
	return &raw{sql: sql, args: args}
}

func NewQueryBuilder[OUTPUT any](s *schema) *QueryBuilder[OUTPUT] {
	return &QueryBuilder[OUTPUT]{schema: s}
}

type insertStmt struct {
	PlaceHolderGenerator func(n int) []string
	Table                string
	Columns              []string
	Values               [][]any
	Returning            string
}

func (i insertStmt) flatValues() []any {
	values := make([]any, 0, len(i.Values)*len(i.Values[0]))
	for _, row := range i.Values {
		values = append(values, row...)
	}

	return values
}

func (i insertStmt) getValuesStr() string {
	phs := i.PlaceHolderGenerator(len(i.Values) * len(i.Values[0]))

	output := make([]string, 0, len(i.Values))
	for _, valueRow := range i.Values {
		output = append(output, fmt.Sprintf("(%s)", strings.Join(phs[:len(valueRow)], ",")))
		phs = phs[len(valueRow):]
	}

	return strings.Join(output, ",")
}

func (i insertStmt) ToSql() (string, []any) {
	base := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		i.Table,
		strings.Join(i.Columns, ","),
		i.getValuesStr(),
	)

	if i.Returning != "" {
		base += "RETURNING " + i.Returning
	}

	return base, i.flatValues()
}

func postgresPlaceholder(n int) []string {
	output := make([]string, 0, n)
	for i := 1; i < n+1; i++ {
		output = append(output, fmt.Sprintf("$%d", i))
	}

	return output
}

func questionMarks(n int) []string {
	output := make([]string, 0, n)
	for range n {
		output = append(output, "?")
	}

	return output
}
