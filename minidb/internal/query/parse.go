package query

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/celanwang/miniinfra/minidb/internal/engine"
)

var (
	tokenPattern      = regexp.MustCompile("^(?:[[:space:]]+|[a-zA-Z_][a-zA-Z_0-9]*|[0-9]+|'(?:''|[^'])*'|<=|>=|<>|!=|[(),.*=;<>+?-])")
	identifierPattern = regexp.MustCompile("^[a-zA-Z_][a-zA-Z_0-9]*$")
)

type parser struct {
	tokens []string
	args   []engine.Value
	err    error
}

type statement struct {
	verb        string
	schema      engine.Schema
	table       source
	joins       []join
	filter      []predicate
	projections []expression
	columns     []string
	values      [][]expression
	updates     []updateExpression
}

type source struct{ name, alias string }
type join struct {
	table source
	on    []predicate
}
type expression struct {
	column    string
	qualifier string
	value     engine.Value
}
type predicate struct {
	left, right expression
	operator    string
}
type arithmetic struct {
	left, right expression
	operator    string
}

// UPDATE expressions support a single addition or subtraction.
type updateExpression struct {
	column string
	value  arithmetic
}

func parse(sql string, args []engine.Value) (statement, error) {
	p := parser{args: args}
	for sql != "" {
		token := tokenPattern.FindString(sql)
		if token == "" {
			return statement{}, fmt.Errorf("unexpected SQL input near %.24q", sql)
		}
		if strings.TrimSpace(token) != "" {
			if token[0] != '\'' {
				token = strings.ToLower(token)
			}
			p.tokens = append(p.tokens, token)
		}
		sql = sql[len(token):]
	}

	s := statement{verb: p.next()}
	switch s.verb {
	case "begin", "commit", "rollback":
	case "create":
		p.require("table")
		s.schema = p.schema()
	case "insert":
		p.require("into")
		s.table.name = p.name()
		if p.peek() == "(" {
			s.columns = p.columnList()
		}
		p.require("values")
		for {
			p.require("(")
			var row []expression
			for {
				row = append(row, p.atom())
				if !p.accept(",") {
					break
				}
			}
			p.require(")")
			s.values = append(s.values, row)
			if !p.accept(",") {
				break
			}
		}
	case "update":
		s.table.name = p.name()
		p.require("set")
		for {
			set := updateExpression{column: p.name()}
			p.require("=")
			set.value.left = p.atom()
			if p.peek() == "+" || p.peek() == "-" {
				set.value.operator = p.next()
				set.value.right = p.atom()
			}
			s.updates = append(s.updates, set)
			if !p.accept(",") {
				break
			}
		}
		p.require("where")
		s.filter = p.predicates()
	case "delete":
		p.require("from")
		s.table.name = p.name()
		p.require("where")
		s.filter = p.predicates()
	case "select":
		if !p.accept("*") {
			for {
				s.projections = append(s.projections, p.atom())
				if !p.accept(",") {
					break
				}
			}
		}
		p.require("from")
		s.table = p.source()
		for p.peek() == "join" || p.peek() == "inner" {
			p.accept("inner")
			p.require("join")
			relation := p.source()
			p.require("on")
			s.joins = append(s.joins, join{table: relation, on: p.predicates()})
		}
		if p.accept("where") {
			s.filter = p.predicates()
		}
	default:
		p.fail("unsupported statement %q", s.verb)
	}
	p.accept(";")
	if p.peek() != "" {
		p.fail("unexpected token %q", p.peek())
	}
	if len(p.args) != 0 {
		p.fail("unused query arguments")
	}
	return s, p.err
}

func (p *parser) schema() engine.Schema {
	s := engine.Schema{Name: p.name()}
	type pendingIndex struct {
		name    string
		columns []string
		primary bool
		unique  bool
	}
	var indexes []pendingIndex
	p.require("(")
	for {
		switch p.peek() {
		case "primary":
			p.next()
			p.require("key")
			indexes = append(indexes, pendingIndex{name: "primary", columns: p.columnList(), primary: true, unique: true})
		case "unique":
			p.next()
			names := p.columnList()
			indexes = append(indexes, pendingIndex{name: strings.Join(names, ",") + "_key", columns: names, unique: true})
		case "index":
			p.next()
			name := p.name()
			indexes = append(indexes, pendingIndex{name: name, columns: p.columnList()})
		default:
			column := engine.Column{Name: p.name()}
			switch typ := p.next(); typ {
			case "int", "integer", "bigint":
				column.Kind = engine.Integer
			case "text":
				column.Kind = engine.Text
			case "bool", "boolean":
				column.Kind = engine.Boolean
			default:
				p.fail("unsupported column type %q", typ)
			}
			for {
				switch p.peek() {
				case "primary":
					p.next()
					p.require("key")
					indexes = append(indexes, pendingIndex{name: "primary", columns: []string{column.Name}, primary: true, unique: true})
				case "not":
					p.next()
					p.require("null")
					column.NotNull = true
				case "unique":
					p.next()
					indexes = append(indexes, pendingIndex{name: column.Name + "_key", columns: []string{column.Name}, unique: true})
				default:
					goto columnDone
				}
			}
		columnDone:
			s.Columns = append(s.Columns, column)
		}
		if !p.accept(",") {
			break
		}
	}
	p.require(")")
	for _, def := range indexes {
		idx := engine.Index{Name: def.name, Primary: def.primary, Unique: def.unique}
		for _, name := range def.columns {
			column := s.Column(name)
			if column < 0 {
				p.fail("unknown indexed column %q", name)
				continue
			}
			if def.primary {
				s.Columns[column].NotNull = true
			}
			idx.Columns = append(idx.Columns, column)
		}
		s.Indexes = append(s.Indexes, idx)
	}
	return s
}

func (p *parser) columnList() []string {
	p.require("(")
	names := []string{p.name()}
	for p.accept(",") {
		names = append(names, p.name())
	}
	p.require(")")
	return names
}

func (p *parser) source() source {
	name := p.name()
	result := source{name: name, alias: name}
	if p.accept("as") {
		result.alias = p.name()
	} else if identifierPattern.MatchString(p.peek()) {
		switch p.peek() {
		case "where", "join", "inner", "on":
		default:
			result.alias = p.next()
		}
	}
	return result
}

func (p *parser) predicates() []predicate {
	var result []predicate
	for {
		left, op := p.atom(), p.next()
		switch op {
		case "=", "<>", "!=", "<", ">", "<=", ">=":
		default:
			p.fail("unsupported comparison %q", op)
		}
		result = append(result, predicate{left: left, right: p.atom(), operator: op})
		if !p.accept("and") {
			return result
		}
	}
}

func (p *parser) atom() expression {
	token := p.next()
	switch token {
	case "?":
		if len(p.args) == 0 {
			p.fail("missing query argument")
			return expression{}
		}
		v := p.args[0]
		p.args = p.args[1:]
		return expression{value: v}
	case "null":
		return expression{}
	case "true", "false":
		return expression{value: engine.Bool(token == "true")}
	case "-":
		token += p.next()
	}
	if strings.HasPrefix(token, "'") {
		return expression{value: engine.String(strings.ReplaceAll(token[1:len(token)-1], "''", "'"))}
	}
	if token != "" && (token[0] == '-' || token[0] >= '0' && token[0] <= '9') {
		number, err := strconv.ParseInt(token, 10, 64)
		if err != nil {
			p.fail("invalid integer %q", token)
		}
		return expression{value: engine.Int(number)}
	}
	if !identifierPattern.MatchString(token) {
		p.fail("expected value or column, got %q", token)
		return expression{}
	}
	if p.accept(".") {
		return expression{qualifier: token, column: p.name()}
	}
	return expression{column: token}
}

func (p *parser) peek() string {
	if len(p.tokens) == 0 {
		return ""
	}
	return p.tokens[0]
}

func (p *parser) next() string {
	token := p.peek()
	if token != "" {
		p.tokens = p.tokens[1:]
	}
	return token
}

func (p *parser) accept(token string) bool {
	if p.peek() == token {
		p.next()
		return true
	}
	return false
}

func (p *parser) require(token string) {
	if !p.accept(token) {
		p.fail("expected %q, got %q", token, p.peek())
	}
}

func (p *parser) name() string {
	token := p.next()
	if !identifierPattern.MatchString(token) {
		p.fail("expected identifier, got %q", token)
	}
	return token
}

func (p *parser) fail(format string, args ...any) {
	if p.err == nil {
		p.err = fmt.Errorf(format, args...)
	}
}
