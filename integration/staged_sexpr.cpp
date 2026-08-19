// Canonical s-expr serialization of the logical-plan artifacts. See the header.
#include "staged_sexpr.hpp"

#include "db25/ast/node_types.hpp"
#include "db25/plan/expr_ir.hpp"
#include "db25/plan/logical_plan.hpp"

#include <string>
#include <variant>

namespace db25::staged {
namespace {

using db25::plan::Expr;
using db25::plan::ExprKind;
using db25::plan::LogicalNode;
using db25::plan::LogicalOp;
using db25::plan::ColumnSchema;
using db25::plan::SortKeyIR;

// ---- small renderers -------------------------------------------------------

// An s-expr atom for an identifier: bare when it is a plain identifier, else
// double-quoted (with embedded quotes/backslashes escaped) so any name is
// representable unambiguously.
std::string ident(const std::string& s) {
    bool bare = !s.empty();
    for (const char c : s) {
        const bool ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                        (c >= '0' && c <= '9') || c == '_';
        if (!ok) { bare = false; break; }
    }
    if (bare) return s;
    std::string out = "\"";
    for (const char c : s) {
        if (c == '"' || c == '\\') out.push_back('\\');
        out.push_back(c);
    }
    out.push_back('"');
    return out;
}

const char* datatype(db25::ast::DataType t) { return db25::ast::data_type_to_string(t); }

const char* join_type(db25::ast::JoinType j) {
    switch (j) {
        case db25::ast::JoinType::Inner: return "inner";
        case db25::ast::JoinType::Left: return "left";
        case db25::ast::JoinType::Right: return "right";
        case db25::ast::JoinType::Full: return "full";
        case db25::ast::JoinType::Cross: return "cross";
        case db25::ast::JoinType::Lateral: return "lateral";
    }
    return "?";
}

const char* set_op(db25::ast::SetOp s) {
    switch (s) {
        case db25::ast::SetOp::Union: return "union";
        case db25::ast::SetOp::UnionAll: return "union-all";
        case db25::ast::SetOp::Intersect: return "intersect";
        case db25::ast::SetOp::Except: return "except";
        default: break;
    }
    // ALL-variants and any future enumerators: fall back to the numeric tag so
    // the rendering stays total rather than silently collapsing to one value.
    return "setop";
}

// Expr 2-bit nullability -> t / f / ? (nullable / not-null / unknown).
const char* null2(std::uint8_t n) { return n == 2 ? "t" : (n == 1 ? "f" : "?"); }

std::string literal_value(const Expr& e) {
    const auto& v = e.value.value;
    if (std::holds_alternative<std::monostate>(v)) return "null";
    if (const auto* i = std::get_if<std::int64_t>(&v)) return std::to_string(*i);
    if (const auto* d = std::get_if<double>(&v)) return std::to_string(*d);
    if (const auto* b = std::get_if<bool>(&v)) return *b ? "true" : "false";
    if (const auto* s = std::get_if<std::string>(&v)) return ident(*s);
    return "?";
}

// ---- expression rendering (inline) -----------------------------------------

void render_expr(const Expr& e, std::string& out);

void render_sort_key(const SortKeyIR& k, std::string& out) {
    out.append("(key ");
    if (k.expr) render_expr(*k.expr, out); else out.append("null");
    out.append(k.descending ? " desc" : " asc");
    if (k.nulls_order_explicit) out.append(k.nulls_first ? " nulls-first" : " nulls-last");
    out.push_back(')');
}

void render_children(const Expr& e, std::string& out) {
    for (const auto& c : e.children) {
        out.push_back(' ');
        if (c) render_expr(*c, out); else out.append("null");
    }
}

// Type + nullability suffix carried on every Expr node.
void render_type_suffix(const Expr& e, std::string& out) {
    out.append(" :type ");
    out.append(datatype(e.type));
    out.append(" :null ");
    out.append(null2(e.nullability));
}

void render_expr(const Expr& e, std::string& out) {
    switch (e.kind) {
        case ExprKind::Literal:
            out.append("(lit ");
            out.append(literal_value(e));
            render_type_suffix(e, out);
            out.push_back(')');
            return;
        case ExprKind::ColumnRef:
            out.append("(colref ");
            out.append(std::to_string(e.input_index));
            render_type_suffix(e, out);
            out.push_back(')');
            return;
        case ExprKind::OuterRef:
            out.append("(outerref ");
            out.append(std::to_string(e.input_index));
            out.append(" :depth ");
            out.append(std::to_string(e.outer_depth));
            render_type_suffix(e, out);
            out.push_back(')');
            return;
        case ExprKind::BinaryOp:
            out.append("(binop ");
            out.append(ident(db25::ast::binary_op_to_string(e.bin_op)));
            render_children(e, out);
            render_type_suffix(e, out);
            out.push_back(')');
            return;
        case ExprKind::UnaryOp:
            out.append("(unop ");
            out.append(ident(db25::ast::unary_op_to_string(e.un_op)));
            render_children(e, out);
            render_type_suffix(e, out);
            out.push_back(')');
            return;
        case ExprKind::ScalarFunction:
            out.append("(func ");
            out.append(ident(e.func_name));
            render_children(e, out);
            render_type_suffix(e, out);
            out.push_back(')');
            return;
        case ExprKind::Aggregate:
            out.append("(agg ");
            out.append(ident(e.func_name));
            if (e.distinct) out.append(" :distinct");
            render_children(e, out);
            if (e.filter) { out.append(" :filter "); render_expr(*e.filter, out); }
            render_type_suffix(e, out);
            out.push_back(')');
            return;
        case ExprKind::WindowFunction:
            out.append("(winfunc ");
            out.append(ident(e.func_name));
            render_children(e, out);
            out.append(" :over (");
            {
                bool first = true;
                out.append(":partition (");
                for (const auto& p : e.window.partition_by) {
                    if (!first) out.push_back(' ');
                    first = false;
                    if (p) render_expr(*p, out);
                }
                out.append(") :order (");
                first = true;
                for (const auto& k : e.window.order_by) {
                    if (!first) out.push_back(' ');
                    first = false;
                    render_sort_key(k, out);
                }
                out.push_back(')');
                if (e.window.frame.present) {
                    out.append(" :frame ");
                    out.append(ident(e.window.frame.spec));
                }
            }
            out.push_back(')');
            render_type_suffix(e, out);
            out.push_back(')');
            return;
        case ExprKind::Cast:
            out.append("(cast ");
            render_children(e, out);
            out.append(" :to ");
            out.append(datatype(e.target_type));
            if (e.type_precision != 0) {
                out.append(" :prec ");
                out.append(std::to_string(e.type_precision));
                out.append(" :scale ");
                out.append(std::to_string(e.type_scale));
            }
            if (e.type_length != 0) {
                out.append(" :len ");
                out.append(std::to_string(e.type_length));
            }
            render_type_suffix(e, out);
            out.push_back(')');
            return;
        case ExprKind::Between:
            out.append("(between");
            if (e.negated()) out.append(" :negated");
            render_children(e, out);
            render_type_suffix(e, out);
            out.push_back(')');
            return;
        case ExprKind::Like:
            out.append("(like");
            if (e.case_insensitive()) out.append(" :ci");
            if (e.negated()) out.append(" :negated");
            render_children(e, out);
            render_type_suffix(e, out);
            out.push_back(')');
            return;
        case ExprKind::IsNull:
            out.append("(isnull");
            if (e.negated()) out.append(" :negated");
            render_children(e, out);
            render_type_suffix(e, out);
            out.push_back(')');
            return;
        case ExprKind::BooleanTest:
            out.append("(booltest");
            if (e.negated()) out.append(" :negated");
            out.append(" :is ");
            out.append(e.bool_test == db25::plan::BoolTest::True
                           ? "true"
                           : (e.bool_test == db25::plan::BoolTest::False ? "false" : "unknown"));
            render_children(e, out);
            render_type_suffix(e, out);
            out.push_back(')');
            return;
        case ExprKind::Case:
            out.append("(case");
            render_children(e, out);
            render_type_suffix(e, out);
            out.push_back(')');
            return;
        case ExprKind::InList:
            out.append("(inlist");
            if (e.negated()) out.append(" :negated");
            render_children(e, out);
            render_type_suffix(e, out);
            out.push_back(')');
            return;
        case ExprKind::Subquery:
            out.append("(subquery :kind ");
            out.append(e.subquery_kind == db25::plan::SubqueryKind::Scalar
                           ? "scalar"
                           : (e.subquery_kind == db25::plan::SubqueryKind::In ? "in" : "exists"));
            if (e.correlated) out.append(" :correlated");
            if (e.negated()) out.append(" :negated");
            render_type_suffix(e, out);
            out.push_back(')');  // sub_plan rendered by the node layer, not inline
            return;
        case ExprKind::Parameter:
            out.append("(param ");
            out.append(std::to_string(e.param_index));
            render_type_suffix(e, out);
            out.push_back(')');
            return;
        default:
            out.append("(expr ");
            out.append(db25::plan::expr_kind_to_string(e.kind));
            render_children(e, out);
            render_type_suffix(e, out);
            out.push_back(')');
            return;
    }
}

// ---- schema + node rendering -----------------------------------------------

void render_schema(const db25::plan::Schema& schema, std::string& out) {
    out.push_back('(');
    bool first = true;
    for (const ColumnSchema& c : schema) {
        if (!first) out.push_back(' ');
        first = false;
        out.push_back('(');
        out.append(ident(c.name));
        out.push_back(' ');
        out.append(datatype(c.type));
        out.append(" :null ");
        out.append(c.nullable ? "t" : "f");
        if (!c.alias.empty() && c.alias != c.name) {
            out.append(" :alias ");
            out.append(ident(c.alias));
        }
        if (c.hidden) out.append(" :hidden");
        out.push_back(')');
    }
    out.push_back(')');
}

void indent(std::string& out, int depth) {
    for (int i = 0; i < depth; ++i) out.append("  ");
}

void render_expr_list(const char* label, const std::vector<db25::plan::ExprPtr>& xs,
                      std::string& out) {
    out.push_back(' ');
    out.append(label);
    out.append(" (");
    bool first = true;
    for (const auto& x : xs) {
        if (!first) out.push_back(' ');
        first = false;
        if (x) render_expr(*x, out);
    }
    out.push_back(')');
}

void render_node(const LogicalNode* n, int depth, std::string& out) {
    indent(out, depth);
    if (n == nullptr) { out.append("(null)"); return; }

    out.push_back('(');
    out.append(db25::plan::logical_op_to_string(n->op));

    // Per-op payload (only the fields the op uses).
    switch (n->op) {
        case LogicalOp::Scan:
        case LogicalOp::WorkingTableScan:
            out.append(" :rel ");
            out.append(ident(n->table_name));
            if (!n->alias.empty() && n->alias != n->table_name) {
                out.append(" :alias ");
                out.append(ident(n->alias));
            }
            break;
        case LogicalOp::Join:
            out.append(" :kind ");
            out.append(join_type(n->join_type));
            if (n->predicate) { out.append(" :pred "); render_expr(*n->predicate, out); }
            break;
        case LogicalOp::Filter:
            if (n->predicate) { out.append(" :pred "); render_expr(*n->predicate, out); }
            break;
        case LogicalOp::Project:
            render_expr_list(":exprs", n->exprs, out);
            break;
        case LogicalOp::Aggregate:
            render_expr_list(":keys", n->group_keys, out);
            render_expr_list(":aggs", n->aggregates, out);
            break;
        case LogicalOp::Window:
            render_expr_list(":windows", n->window_functions, out);
            break;
        case LogicalOp::Sort:
            out.append(" :keys (");
            {
                bool first = true;
                for (const SortKeyIR& k : n->sort_keys) {
                    if (!first) out.push_back(' ');
                    first = false;
                    render_sort_key(k, out);
                }
            }
            out.push_back(')');
            break;
        case LogicalOp::SetOp:
            out.append(" :op ");
            out.append(set_op(n->set_op));
            break;
        case LogicalOp::RecursiveCTE:
            out.append(" :name ");
            out.append(ident(n->table_name));
            out.append(" :op ");
            out.append(set_op(n->set_op));
            break;
        case LogicalOp::Values:
            out.append(" :rows ");
            out.append(std::to_string(n->value_rows.size()));
            break;
        default:
            break;
    }

    if (n->has_limit) { out.append(" :limit "); out.append(std::to_string(n->limit)); }
    if (n->has_offset) { out.append(" :offset "); out.append(std::to_string(n->offset)); }

    out.append(" :out ");
    render_schema(n->output, out);

    for (const auto& c : n->children) {
        out.push_back('\n');
        render_node(c.get(), depth + 1, out);
    }
    out.push_back(')');
}

}  // namespace

std::string plan_to_sexpr(const LogicalNode* root) {
    std::string out;
    render_node(root, 0, out);
    return out;
}

std::string expr_to_sexpr(const Expr& e) {
    std::string out;
    render_expr(e, out);
    return out;
}

}  // namespace db25::staged
