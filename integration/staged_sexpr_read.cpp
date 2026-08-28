// Phase B plan reader: canonical plan s-expr -> owned LogicalNode. See header.
#include "staged_sexpr_read.hpp"

#include "db25/ast/node_types.hpp"
#include "db25/plan/expr_ir.hpp"
#include "db25/plan/logical_plan.hpp"

#include <cstdlib>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace db25::staged {
namespace {

using db25::plan::Expr;
using db25::plan::ExprKind;
using db25::plan::ExprPtr;
using db25::plan::LogicalNode;
using db25::plan::LogicalNodePtr;
using db25::plan::LogicalOp;
using db25::plan::ColumnSchema;

// ---- generic s-expression parse --------------------------------------------

struct SNode {
    bool is_list = false;
    std::string atom;            // when !is_list
    std::vector<SNode> items;    // when is_list
    const SNode* head() const { return (is_list && !items.empty()) ? &items[0] : nullptr; }
};

struct Parser {
    std::string_view s;
    std::size_t i = 0;

    void skip_ws() {
        while (i < s.size() && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r')) ++i;
    }
    // Parse one node; sets ok=false on malformed input.
    SNode parse(bool& ok) {
        skip_ws();
        if (i >= s.size()) { ok = false; return {}; }
        if (s[i] == '(') {
            ++i;
            SNode n;
            n.is_list = true;
            while (true) {
                skip_ws();
                if (i >= s.size()) { ok = false; return n; }
                if (s[i] == ')') { ++i; break; }
                SNode c = parse(ok);
                if (!ok) return n;
                n.items.push_back(std::move(c));
            }
            return n;
        }
        if (s[i] == '"') {  // quoted atom, with \" and \\ escapes
            ++i;
            SNode n;
            while (i < s.size() && s[i] != '"') {
                if (s[i] == '\\' && i + 1 < s.size()) ++i;
                n.atom.push_back(s[i]);
                ++i;
            }
            if (i >= s.size()) { ok = false; return n; }
            ++i;  // closing quote
            return n;
        }
        SNode n;  // bare atom
        while (i < s.size() && s[i] != ' ' && s[i] != '\t' && s[i] != '\n' &&
               s[i] != '\r' && s[i] != '(' && s[i] != ')') {
            n.atom.push_back(s[i]);
            ++i;
        }
        return n;
    }
};

// ---- reverse enum mappings (match the writer's spellings) -------------------

bool datatype_from(std::string_view s, ast::DataType& out) {
    static const ast::DataType c[] = {
        ast::DataType::TinyInt, ast::DataType::SmallInt, ast::DataType::Integer,
        ast::DataType::BigInt, ast::DataType::Decimal, ast::DataType::Real,
        ast::DataType::Double, ast::DataType::Char, ast::DataType::VarChar,
        ast::DataType::Text, ast::DataType::Boolean, ast::DataType::Date,
        ast::DataType::Time, ast::DataType::Timestamp, ast::DataType::Interval,
        ast::DataType::Array, ast::DataType::Unknown};
    for (auto d : c) if (s == ast::data_type_to_string(d)) { out = d; return true; }
    return false;
}

bool binaryop_from(std::string_view s, ast::BinaryOp& out) {
    static const ast::BinaryOp c[] = {
        ast::BinaryOp::Add, ast::BinaryOp::Subtract, ast::BinaryOp::Multiply,
        ast::BinaryOp::Divide, ast::BinaryOp::Modulo, ast::BinaryOp::Equal,
        ast::BinaryOp::NotEqual, ast::BinaryOp::LessThan, ast::BinaryOp::LessEqual,
        ast::BinaryOp::GreaterThan, ast::BinaryOp::GreaterEqual, ast::BinaryOp::And,
        ast::BinaryOp::Or, ast::BinaryOp::Concat, ast::BinaryOp::Like,
        ast::BinaryOp::NotLike, ast::BinaryOp::BitAnd, ast::BinaryOp::BitOr,
        ast::BinaryOp::BitXor, ast::BinaryOp::BitShiftLeft, ast::BinaryOp::BitShiftRight,
        ast::BinaryOp::Is, ast::BinaryOp::IsNot, ast::BinaryOp::In,
        ast::BinaryOp::NotIn, ast::BinaryOp::IsDistinctFrom};
    for (auto b : c) if (s == ast::binary_op_to_string(b)) { out = b; return true; }
    return false;
}

bool unaryop_from(std::string_view s, ast::UnaryOp& out) {
    static const ast::UnaryOp c[] = {
        ast::UnaryOp::Not, ast::UnaryOp::Negate, ast::UnaryOp::BitwiseNot,
        ast::UnaryOp::IsNull, ast::UnaryOp::IsNotNull, ast::UnaryOp::Exists,
        ast::UnaryOp::NotExists};
    for (auto u : c) if (s == ast::unary_op_to_string(u)) { out = u; return true; }
    return false;
}

bool logicalop_from(std::string_view s, LogicalOp& out) {
    static const LogicalOp c[] = {
        LogicalOp::Scan, LogicalOp::Filter, LogicalOp::Project, LogicalOp::Join,
        LogicalOp::SemiJoin, LogicalOp::AntiJoin, LogicalOp::Aggregate,
        LogicalOp::Window, LogicalOp::Distinct, LogicalOp::Sort, LogicalOp::Limit,
        LogicalOp::SetOp, LogicalOp::Values, LogicalOp::Insert, LogicalOp::Update,
        LogicalOp::Delete, LogicalOp::Returning, LogicalOp::RecursiveCTE,
        LogicalOp::WorkingTableScan};
    for (auto o : c) if (s == db25::plan::logical_op_to_string(o)) { out = o; return true; }
    return false;
}

bool jointype_from(std::string_view s, ast::JoinType& out) {
    if (s == "inner") { out = ast::JoinType::Inner; return true; }
    if (s == "left") { out = ast::JoinType::Left; return true; }
    if (s == "right") { out = ast::JoinType::Right; return true; }
    if (s == "full") { out = ast::JoinType::Full; return true; }
    if (s == "cross") { out = ast::JoinType::Cross; return true; }
    if (s == "lateral") { out = ast::JoinType::Lateral; return true; }
    return false;
}

std::uint8_t null_from(std::string_view s) { return s == "t" ? 2 : (s == "f" ? 1 : 0); }

// ---- expression builder ----------------------------------------------------

// Apply trailing `:type T :null x` (present on every Expr node) to `e`.
void apply_type_null(Expr& e, const SNode& list) {
    for (std::size_t k = 1; k + 1 < list.items.size(); ++k) {
        const std::string& kw = list.items[k].atom;
        if (kw == ":type") { ast::DataType t; if (datatype_from(list.items[k + 1].atom, t)) e.type = t; }
        else if (kw == ":null") e.nullability = null_from(list.items[k + 1].atom);
    }
}

ExprPtr expr_from(const SNode& n, std::string& err);

// A keyword flag that carries NO value (a bare `:flag`), vs. the value-ful
// keywords (`:type T`, `:over (...)`, `:filter e`, ...). Operand collection must
// NOT consume a value after a valueless flag, or it would swallow a following
// child operand (e.g. `(agg SUM :distinct (colref 0) …)` would drop the arg).
bool is_valueless_flag(const std::string& a) {
    return a == ":distinct" || a == ":ci" || a == ":negated" || a == ":correlated";
}

// Collect positional operand child-lists: skip valueless flags (no value), skip
// a value-ful keyword together with its value (which may itself be a list, e.g.
// `:over (...)` / `:filter (expr)` - never an operand), and collect the rest.
std::vector<const SNode*> positional_operands(const SNode& n, std::size_t start) {
    std::vector<const SNode*> out;
    for (std::size_t k = start; k < n.items.size(); ++k) {
        const SNode& it = n.items[k];
        if (!it.is_list && !it.atom.empty() && it.atom[0] == ':') {
            if (is_valueless_flag(it.atom)) continue;
            ++k;  // skip this keyword's value (atom or list)
            continue;
        }
        if (it.is_list) out.push_back(&n.items[k]);
    }
    return out;
}

bool setop_from(std::string_view s, ast::SetOp& out) {
    if (s == "union") { out = ast::SetOp::Union; return true; }
    if (s == "union-all") { out = ast::SetOp::UnionAll; return true; }
    if (s == "intersect") { out = ast::SetOp::Intersect; return true; }
    if (s == "except") { out = ast::SetOp::Except; return true; }
    return false;
}

bool subquerykind_from(std::string_view s, db25::plan::SubqueryKind& out) {
    if (s == "scalar") { out = db25::plan::SubqueryKind::Scalar; return true; }
    if (s == "in") { out = db25::plan::SubqueryKind::In; return true; }
    if (s == "exists") { out = db25::plan::SubqueryKind::Exists; return true; }
    if (s == "quantified") { out = db25::plan::SubqueryKind::Quantified; return true; }
    return false;
}

// Parse one `(key <expr>|null asc|desc [nulls-first|nulls-last])` sort key.
bool sort_key_from(const SNode& n, db25::plan::SortKeyIR& out, std::string& err) {
    if (!n.is_list || n.items.empty() || n.items[0].atom != "key") {
        err = "sort key: expected (key …)";
        return false;
    }
    for (std::size_t k = 1; k < n.items.size(); ++k) {
        const SNode& it = n.items[k];
        if (it.is_list) { out.expr = expr_from(it, err); if (!out.expr) return false; }
        else if (it.atom == "asc") out.descending = false;
        else if (it.atom == "desc") out.descending = true;
        else if (it.atom == "nulls-first") { out.nulls_order_explicit = true; out.nulls_first = true; }
        else if (it.atom == "nulls-last") { out.nulls_order_explicit = true; out.nulls_first = false; }
        // "null" (a valueless expr) leaves out.expr null.
    }
    return true;
}

// Parse a window OVER spec list: (:partition (e…) :order ((key …)…) [:frame S]).
bool window_spec_from(const SNode& over, db25::plan::WindowSpecIR& out, std::string& err) {
    if (!over.is_list) { err = "window: expected (:partition … :order …)"; return false; }
    for (std::size_t k = 0; k + 1 < over.items.size(); ++k) {
        const std::string& kw = over.items[k].atom;
        const SNode& val = over.items[k + 1];
        if (kw == ":partition") {
            for (const SNode& x : val.items) { auto e = expr_from(x, err); if (!e) return false; out.partition_by.push_back(std::move(e)); }
            ++k;
        } else if (kw == ":order") {
            for (const SNode& x : val.items) { db25::plan::SortKeyIR sk; if (!sort_key_from(x, sk, err)) return false; out.order_by.push_back(std::move(sk)); }
            ++k;
        } else if (kw == ":frame") {
            out.frame.present = true;
            out.frame.spec = val.atom;
            ++k;
        }
    }
    return true;
}

ExprPtr expr_from(const SNode& n, std::string& err) {
    if (!n.is_list || n.items.empty()) { err = "expr: expected a list"; return nullptr; }
    const std::string& kind = n.items[0].atom;

    if (kind == "lit") {
        auto e = std::make_unique<Expr>(ExprKind::Literal);
        const std::string& v = n.items.size() > 1 ? n.items[1].atom : "";
        if (v == "null") { /* monostate */ }
        else if (v == "true") e->value.value = true;
        else if (v == "false") e->value.value = false;
        else if (!v.empty() && (v[0] == '-' || (v[0] >= '0' && v[0] <= '9')) &&
                 v.find_first_not_of("-0123456789") == std::string::npos) {
            e->value.value = static_cast<std::int64_t>(std::strtoll(v.c_str(), nullptr, 10));
        } else if (v.find('.') != std::string::npos &&
                   v.find_first_not_of("-0123456789.eE+") == std::string::npos) {
            e->value.value = std::strtod(v.c_str(), nullptr);
        } else {
            e->value.value = v;  // string literal (the quoted atom)
        }
        apply_type_null(*e, n);
        return e;
    }
    if (kind == "colref" || kind == "outerref") {
        auto e = std::make_unique<Expr>(kind == "colref" ? ExprKind::ColumnRef : ExprKind::OuterRef);
        if (n.items.size() > 1) e->input_index = static_cast<std::uint32_t>(std::strtoul(n.items[1].atom.c_str(), nullptr, 10));
        for (std::size_t k = 2; k + 1 < n.items.size(); ++k) {
            const std::string& kw = n.items[k].atom;
            if (kw == ":depth") e->outer_depth = static_cast<std::uint32_t>(std::strtoul(n.items[k + 1].atom.c_str(), nullptr, 10));
            // Column-reference provenance (see render_ref_prov): restore the
            // (table_id, column_id) so a read-back colref is identical to the
            // bound one, not one with the provenance dropped.
            else if (kw == ":tid") e->ref_table_id = static_cast<std::uint32_t>(std::strtoul(n.items[k + 1].atom.c_str(), nullptr, 10));
            else if (kw == ":cid") e->ref_column_id = static_cast<std::uint32_t>(std::strtoul(n.items[k + 1].atom.c_str(), nullptr, 10));
        }
        apply_type_null(*e, n);
        return e;
    }
    if (kind == "binop") {
        auto e = std::make_unique<Expr>(ExprKind::BinaryOp);
        if (n.items.size() < 2 || !binaryop_from(n.items[1].atom, e->bin_op)) { err = "binop: bad operator '" + (n.items.size() > 1 ? n.items[1].atom : "") + "'"; return nullptr; }
        for (const SNode* op : positional_operands(n, 2)) {
            auto c = expr_from(*op, err); if (!c) return nullptr; e->children.push_back(std::move(c));
        }
        apply_type_null(*e, n);
        return e;
    }
    if (kind == "unop") {
        auto e = std::make_unique<Expr>(ExprKind::UnaryOp);
        if (n.items.size() < 2 || !unaryop_from(n.items[1].atom, e->un_op)) { err = "unop: bad operator"; return nullptr; }
        for (const SNode* op : positional_operands(n, 2)) { auto c = expr_from(*op, err); if (!c) return nullptr; e->children.push_back(std::move(c)); }
        apply_type_null(*e, n);
        return e;
    }
    if (kind == "func" || kind == "agg" || kind == "winfunc") {
        const ExprKind ek = kind == "func"    ? ExprKind::ScalarFunction
                            : kind == "agg"   ? ExprKind::Aggregate
                                              : ExprKind::WindowFunction;
        auto e = std::make_unique<Expr>(ek);
        if (n.items.size() > 1) e->func_name = n.items[1].atom;
        for (std::size_t k = 2; k < n.items.size(); ++k)
            if (n.items[k].atom == ":distinct") e->distinct = true;
        for (const SNode* op : positional_operands(n, 2)) { auto c = expr_from(*op, err); if (!c) return nullptr; e->children.push_back(std::move(c)); }
        // Aggregate FILTER (WHERE p) and WindowFunction OVER spec are value-ful
        // keywords positional_operands already skipped; reconstruct them here.
        for (std::size_t k = 2; k + 1 < n.items.size(); ++k) {
            if (n.items[k].atom == ":filter") { e->filter = expr_from(n.items[k + 1], err); if (!e->filter) return nullptr; }
            else if (n.items[k].atom == ":over") { if (!window_spec_from(n.items[k + 1], e->window, err)) return nullptr; }
        }
        apply_type_null(*e, n);
        return e;
    }
    // NOT-flavor / case-insensitive flags carried as bare `:negated` / `:ci`.
    const auto apply_flags = [&](Expr& e) {
        for (std::size_t k = 1; k < n.items.size(); ++k) {
            if (n.items[k].atom == ":negated") e.expr_flags |= db25::plan::ExprFlagNegated;
            else if (n.items[k].atom == ":ci") e.expr_flags |= db25::plan::ExprFlagCaseInsensitive;
        }
    };
    if (kind == "cast") {
        auto e = std::make_unique<Expr>(ExprKind::Cast);
        for (const SNode* op : positional_operands(n, 1)) { auto c = expr_from(*op, err); if (!c) return nullptr; e->children.push_back(std::move(c)); }
        for (std::size_t k = 1; k + 1 < n.items.size(); ++k) {
            const std::string& kw = n.items[k].atom;
            if (kw == ":to") { ast::DataType t; if (datatype_from(n.items[k + 1].atom, t)) e->target_type = t; }
            else if (kw == ":prec") e->type_precision = static_cast<std::uint16_t>(std::strtoul(n.items[k + 1].atom.c_str(), nullptr, 10));
            else if (kw == ":scale") e->type_scale = static_cast<std::uint16_t>(std::strtoul(n.items[k + 1].atom.c_str(), nullptr, 10));
            else if (kw == ":len") e->type_length = static_cast<std::uint32_t>(std::strtoul(n.items[k + 1].atom.c_str(), nullptr, 10));
        }
        apply_type_null(*e, n);
        return e;
    }
    if (kind == "between" || kind == "like" || kind == "isnull" || kind == "inlist" || kind == "case") {
        const ExprKind ek = kind == "between" ? ExprKind::Between
                            : kind == "like"  ? ExprKind::Like
                            : kind == "isnull" ? ExprKind::IsNull
                            : kind == "inlist" ? ExprKind::InList
                                               : ExprKind::Case;
        auto e = std::make_unique<Expr>(ek);
        for (const SNode* op : positional_operands(n, 1)) { auto c = expr_from(*op, err); if (!c) return nullptr; e->children.push_back(std::move(c)); }
        apply_flags(*e);
        apply_type_null(*e, n);
        return e;
    }
    if (kind == "booltest") {
        auto e = std::make_unique<Expr>(ExprKind::BooleanTest);
        for (std::size_t k = 1; k + 1 < n.items.size(); ++k)
            if (n.items[k].atom == ":is") {
                const std::string& v = n.items[k + 1].atom;
                e->bool_test = v == "true" ? db25::plan::BoolTest::True
                              : v == "false" ? db25::plan::BoolTest::False
                                             : db25::plan::BoolTest::Unknown;
            }
        for (const SNode* op : positional_operands(n, 1)) { auto c = expr_from(*op, err); if (!c) return nullptr; e->children.push_back(std::move(c)); }
        apply_flags(*e);
        apply_type_null(*e, n);
        return e;
    }
    if (kind == "subquery") {
        auto e = std::make_unique<Expr>(ExprKind::Subquery);
        for (std::size_t k = 1; k < n.items.size(); ++k) {
            const std::string& kw = n.items[k].atom;
            if (kw == ":correlated") e->correlated = true;
            else if (kw == ":kind" && k + 1 < n.items.size()) { subquerykind_from(n.items[k + 1].atom, e->subquery_kind); ++k; }
            else if (kw == ":op" && k + 1 < n.items.size()) { binaryop_from(n.items[k + 1].atom, e->bin_op); ++k; }
            else if (kw == ":quant" && k + 1 < n.items.size()) { if (n.items[k + 1].atom == "all") e->expr_flags |= db25::plan::ExprFlagQuantAll; ++k; }
        }
        apply_flags(*e);
        apply_type_null(*e, n);
        return e;  // sub_plan is not serialized inline (see the writer)
    }
    if (kind == "param") {
        auto e = std::make_unique<Expr>(ExprKind::Parameter);
        if (n.items.size() > 1) e->param_index = static_cast<std::uint32_t>(std::strtoul(n.items[1].atom.c_str(), nullptr, 10));
        apply_type_null(*e, n);
        return e;
    }
    err = "expr: unsupported kind '" + kind + "'";
    return nullptr;
}

// ---- schema + node builder -------------------------------------------------

bool schema_from(const SNode& list, db25::plan::Schema& out, std::string& err) {
    if (!list.is_list) { err = "schema: expected a list"; return false; }
    for (const SNode& col : list.items) {
        if (!col.is_list || col.items.size() < 2) { err = "schema: bad column"; return false; }
        ColumnSchema c;
        c.name = col.items[0].atom;
        if (!datatype_from(col.items[1].atom, c.type)) { err = "schema: bad type '" + col.items[1].atom + "'"; return false; }
        for (std::size_t k = 2; k < col.items.size(); ++k) {
            const std::string& kw = col.items[k].atom;
            if (kw == ":null" && k + 1 < col.items.size()) c.nullable = (col.items[++k].atom == "t");
            else if (kw == ":alias" && k + 1 < col.items.size()) c.alias = col.items[++k].atom;
            // Base-column provenance (see render_schema): restore the catalog
            // (table_id, column_id) so a read-back schema matches the bound plan.
            else if (kw == ":tid" && k + 1 < col.items.size()) c.table_id = static_cast<std::uint32_t>(std::strtoul(col.items[++k].atom.c_str(), nullptr, 10));
            else if (kw == ":cid" && k + 1 < col.items.size()) c.column_id = static_cast<std::uint32_t>(std::strtoul(col.items[++k].atom.c_str(), nullptr, 10));
            else if (kw == ":hidden") c.hidden = true;
        }
        out.push_back(std::move(c));
    }
    return true;
}

LogicalNodePtr node_from(const SNode& n, std::string& err) {
    if (!n.is_list || n.items.empty()) { err = "node: expected a list"; return nullptr; }
    LogicalOp op;
    if (!logicalop_from(n.items[0].atom, op)) { err = "node: unsupported op '" + n.items[0].atom + "'"; return nullptr; }

    auto node = std::make_unique<LogicalNode>(op);

    // Walk items: keyword+value pairs are payload; bare child-lists are children.
    for (std::size_t k = 1; k < n.items.size(); ++k) {
        const SNode& it = n.items[k];
        if (!it.is_list && !it.atom.empty() && it.atom[0] == ':') {
            const std::string kw = it.atom;
            const SNode* val = (k + 1 < n.items.size()) ? &n.items[k + 1] : nullptr;
            if (val == nullptr) { err = "node: dangling keyword " + kw; return nullptr; }
            ++k;
            if (kw == ":rel" || kw == ":name") node->table_name = val->atom;
            else if (kw == ":alias") node->alias = val->atom;
            else if (kw == ":kind") { if (!jointype_from(val->atom, node->join_type)) { err = "node: bad join kind"; return nullptr; } }
            else if (kw == ":limit") { node->has_limit = true; node->limit = std::strtoll(val->atom.c_str(), nullptr, 10); }
            else if (kw == ":offset") { node->has_offset = true; node->offset = std::strtoll(val->atom.c_str(), nullptr, 10); }
            else if (kw == ":out") { if (!schema_from(*val, node->output, err)) return nullptr; }
            else if (kw == ":pred") { auto e = expr_from(*val, err); if (!e) return nullptr; node->predicate = std::move(e); }
            else if (kw == ":exprs") { for (const SNode& x : val->items) { auto e = expr_from(x, err); if (!e) return nullptr; node->exprs.push_back(std::move(e)); } }
            else if (kw == ":keys") {
                // A Sort's :keys are `(key <expr> asc|desc …)` sort keys; every
                // other op's :keys are plain grouping expressions.
                if (op == LogicalOp::Sort) {
                    for (const SNode& x : val->items) { db25::plan::SortKeyIR sk; if (!sort_key_from(x, sk, err)) return nullptr; node->sort_keys.push_back(std::move(sk)); }
                } else {
                    for (const SNode& x : val->items) { auto e = expr_from(x, err); if (!e) return nullptr; node->group_keys.push_back(std::move(e)); }
                }
            }
            else if (kw == ":aggs") { for (const SNode& x : val->items) { auto e = expr_from(x, err); if (!e) return nullptr; node->aggregates.push_back(std::move(e)); } }
            else if (kw == ":windows") { for (const SNode& x : val->items) { auto e = expr_from(x, err); if (!e) return nullptr; node->window_functions.push_back(std::move(e)); } }
            else if (kw == ":op") { if (!setop_from(val->atom, node->set_op)) { err = "node: bad set-op '" + val->atom + "'"; return nullptr; } }
            else if (kw == ":rows") {
                // The writer pins only the ROW COUNT of a Values node (its row
                // exprs are not serialized); reconstruct that many empty rows so
                // the round-trip re-emits the same :rows N.
                const auto count = static_cast<std::size_t>(std::strtoul(val->atom.c_str(), nullptr, 10));
                node->value_rows.resize(count);
            }
            continue;
        }
        if (it.is_list) {  // a child plan node
            auto child = node_from(it, err);
            if (!child) return nullptr;
            node->children.push_back(std::move(child));
        }
    }
    return node;
}

}  // namespace

LogicalNodePtr plan_from_sexpr(std::string_view sexpr, std::string& error) {
    error.clear();
    Parser p{sexpr, 0};
    bool ok = true;
    SNode root = p.parse(ok);
    if (!ok) { error = "malformed s-expression"; return nullptr; }
    return node_from(root, error);
}

}  // namespace db25::staged
