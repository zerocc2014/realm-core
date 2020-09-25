/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include <realm/query.hpp>

#include <realm/array.hpp>
#include <realm/column_fwd.hpp>
#include <realm/db.hpp>
#include <realm/query_engine.hpp>
#include <realm/query_expression.hpp>
#include <realm/table_view.hpp>
#include <realm/table_tpl.hpp>

#include <algorithm>


using namespace realm;
using namespace realm::metrics;

Query::Query()
{
    create();
}

Query::Query(ConstTableRef table, const LnkLst& list)
    : m_table(table.cast_away_const())
    , m_source_link_list(list.clone())
{
    m_view = m_source_link_list.get();
    REALM_ASSERT_DEBUG(list.get_target_table() == m_table);
    create();
}

Query::Query(ConstTableRef table, LnkLstPtr&& ll)
    : m_table(table.cast_away_const())
    , m_source_link_list(std::move(ll))
{
    m_view = m_source_link_list.get();
    REALM_ASSERT_DEBUG(ll->get_target_table() == m_table);
    create();
}

Query::Query(ConstTableRef table, ConstTableView* tv)
    : m_table(table.cast_away_const())
    , m_view(tv)
    , m_source_table_view(tv)
{
    create();
}

Query::Query(ConstTableRef table, std::unique_ptr<ConstTableView> tv)
    : m_table(table.cast_away_const())
    , m_view(tv.get())
    , m_source_table_view(tv.get())
    , m_owned_source_table_view(std::move(tv))
{
    create();
}

void Query::create()
{
    m_groups.emplace_back();
}

Query::Query(const Query& source)
    : error_code(source.error_code)
    , m_groups(source.m_groups)
    , m_table(source.m_table)
{
    if (source.m_owned_source_table_view) {
        m_owned_source_table_view = source.m_owned_source_table_view->clone();
        m_source_table_view = m_owned_source_table_view.get();
    }
    else {
        // FIXME: The lifetime of `m_source_table_view` may be tied to that of `source`, which can easily
        // turn `m_source_table_view` into a dangling reference.
        m_source_table_view = source.m_source_table_view;
        m_source_link_list = source.m_source_link_list ? source.m_source_link_list->clone() : LnkLstPtr{};
    }
    if (m_source_table_view) {
        m_view = m_source_table_view;
    }
    else {
        m_view = m_source_link_list.get();
    }
}

Query& Query::operator=(const Query& source)
{
    if (this != &source) {
        m_groups = source.m_groups;
        m_table = source.m_table;

        if (source.m_owned_source_table_view) {
            m_owned_source_table_view = source.m_owned_source_table_view->clone();
            m_source_table_view = m_owned_source_table_view.get();

            m_source_link_list = nullptr;
        }
        else {
            // FIXME: The lifetime of `m_source_table_view` may be tied to that of `source`, which can easily
            // turn `m_source_table_view` into a dangling reference.
            m_source_table_view = source.m_source_table_view;
            m_owned_source_table_view = nullptr;

            m_source_link_list = source.m_source_link_list ? source.m_source_link_list->clone() : LnkLstPtr{};
        }
        if (m_source_table_view) {
            m_view = m_source_table_view;
        }
        else {
            m_view = m_source_link_list.get();
        }
    }
    return *this;
}

Query::Query(Query&&) = default;
Query& Query::operator=(Query&&) = default;

Query::~Query() noexcept = default;

Query::Query(const Query* source, Transaction* tr, PayloadPolicy policy)
{
    if (source->m_source_table_view) {
        m_owned_source_table_view = tr->import_copy_of(*source->m_source_table_view, policy);
        m_source_table_view = m_owned_source_table_view.get();
        m_view = m_source_table_view;
    }
    else {
        // nothing?
    }
    if (source->m_source_link_list.get()) {
        m_source_link_list = tr->import_copy_of(source->m_source_link_list);
        m_view = m_source_link_list.get();
    }
    m_groups = source->m_groups;
    if (source->m_table)
        set_table(tr->import_copy_of(source->m_table));
    // otherwise: empty query.
}

Query::Query(std::unique_ptr<Expression> expr)
    : Query()
{
    if (auto table = expr->get_base_table())
        set_table(table.cast_away_const());

    add_expression_node(std::move(expr));
}

void Query::set_table(TableRef tr)
{
    if (tr == m_table) {
        return;
    }

    m_table = tr;
    if (m_table) {
        ParentNode* root = root_node();
        if (root)
            root->set_table(m_table);
    }
}


void Query::add_expression_node(std::unique_ptr<Expression> expression)
{
    add_node(std::unique_ptr<ParentNode>(new ExpressionNode(std::move(expression))));
}

// Binary
Query& Query::equal(ColKey column_key, BinaryData b, bool case_sensitive)
{
    if (case_sensitive) {
        add_condition<Equal>(column_key, b);
    }
    else {
        add_condition<EqualIns>(column_key, b);
    }
    return *this;
}
Query& Query::not_equal(ColKey column_key, BinaryData b, bool case_sensitive)
{
    if (case_sensitive) {
        add_condition<NotEqual>(column_key, b);
    }
    else {
        add_condition<NotEqualIns>(column_key, b);
    }
    return *this;
}
Query& Query::begins_with(ColKey column_key, BinaryData b, bool case_sensitive)
{
    if (case_sensitive) {
        add_condition<BeginsWith>(column_key, b);
    }
    else {
        add_condition<BeginsWithIns>(column_key, b);
    }
    return *this;
}
Query& Query::ends_with(ColKey column_key, BinaryData b, bool case_sensitive)
{
    if (case_sensitive) {
        add_condition<EndsWith>(column_key, b);
    }
    else {
        add_condition<EndsWithIns>(column_key, b);
    }
    return *this;
}
Query& Query::contains(ColKey column_key, BinaryData b, bool case_sensitive)
{
    if (case_sensitive) {
        add_condition<Contains>(column_key, b);
    }
    else {
        add_condition<ContainsIns>(column_key, b);
    }
    return *this;
}
Query& Query::like(ColKey column_key, BinaryData b, bool case_sensitive)
{
    if (case_sensitive) {
        add_condition<Like>(column_key, b);
    }
    else {
        add_condition<LikeIns>(column_key, b);
    }
    return *this;
}


namespace {

REALM_NOINLINE REALM_COLD REALM_NORETURN void throw_type_mismatch_error()
{
    throw LogicError{LogicError::type_mismatch};
}

template <class Node>
struct MakeConditionNode {
    static std::unique_ptr<ParentNode> make(ColKey col_key, typename Node::TConditionValue value)
    {
        return std::unique_ptr<ParentNode>{new Node(std::move(value), col_key)};
    }

    static std::unique_ptr<ParentNode> make(ColKey col_key, null)
    {
        return std::unique_ptr<ParentNode>{new Node(null{}, col_key)};
    }

    template <class T = typename Node::TConditionValue>
    static typename std::enable_if<!std::is_same<typename util::RemoveOptional<T>::type, T>::value,
                                   std::unique_ptr<ParentNode>>::type
    make(ColKey col_key, typename util::RemoveOptional<T>::type value)
    {
        return std::unique_ptr<ParentNode>{new Node(std::move(value), col_key)};
    }

    template <class T>
    REALM_FORCEINLINE static std::unique_ptr<ParentNode> make(ColKey, T&&)
    {
        throw_type_mismatch_error();
    }
};

template <class Cond>
struct MakeConditionNode<IntegerNode<ArrayInteger, Cond>> {
    static std::unique_ptr<ParentNode> make(ColKey col_key, int64_t value)
    {
        return std::unique_ptr<ParentNode>{new IntegerNode<ArrayInteger, Cond>(std::move(value), col_key)};
    }

    template <class T>
    REALM_FORCEINLINE static std::unique_ptr<ParentNode> make(ColKey, T&&)
    {
        throw_type_mismatch_error();
    }
};

template <class Cond>
struct MakeConditionNode<StringNode<Cond>> {
    static std::unique_ptr<ParentNode> make(ColKey col_key, StringData value)
    {
        return std::unique_ptr<ParentNode>{new StringNode<Cond>(std::move(value), col_key)};
    }

    static std::unique_ptr<ParentNode> make(ColKey col_key, null)
    {
        return std::unique_ptr<ParentNode>{new StringNode<Cond>(null{}, col_key)};
    }

    template <class T>
    REALM_FORCEINLINE static std::unique_ptr<ParentNode> make(ColKey, T&&)
    {
        throw_type_mismatch_error();
    }
};

template <class Cond, class T>
std::unique_ptr<ParentNode> make_condition_node(const Table& table, ColKey column_key, T value)
{
    table.check_column(column_key);
    DataType type = DataType(column_key.get_type());
    switch (type) {
        case type_Int: {
            if (column_key.get_attrs().test(col_attr_Nullable)) {
                return MakeConditionNode<IntegerNode<ArrayIntNull, Cond>>::make(column_key, value);
            }
            else {
                return MakeConditionNode<IntegerNode<ArrayInteger, Cond>>::make(column_key, value);
            }
        }
        case type_Bool: {
            return MakeConditionNode<BoolNode<Cond>>::make(column_key, value);
        }
        case type_Float: {
            return MakeConditionNode<FloatDoubleNode<ArrayFloat, Cond>>::make(column_key, value);
        }
        case type_Double: {
            return MakeConditionNode<FloatDoubleNode<ArrayDouble, Cond>>::make(column_key, value);
        }
        case type_String: {
            return MakeConditionNode<StringNode<Cond>>::make(column_key, value);
        }
        case type_Binary: {
            return MakeConditionNode<BinaryNode<Cond>>::make(column_key, value);
        }
        case type_Timestamp: {
            return MakeConditionNode<TimestampNode<Cond>>::make(column_key, value);
        }
        case type_Decimal: {
            return MakeConditionNode<DecimalNode<Cond>>::make(column_key, value);
        }
        case type_ObjectId: {
            return MakeConditionNode<ObjectIdNode<Cond>>::make(column_key, value);
        }
        default: {
            throw_type_mismatch_error();
        }
    }
}

template <class Cond>
std::unique_ptr<ParentNode> make_size_condition_node(const Table& table, ColKey column_key, int64_t value)
{
    table.check_column(column_key);
    DataType type = DataType(column_key.get_type());
    ColumnAttrMask attr = column_key.get_attrs();

    if (attr.test(col_attr_List)) {
        switch (type) {
            case type_Int:
            case type_Bool:
            case type_OldDateTime: {
                return std::unique_ptr<ParentNode>{new SizeListNode<int64_t, Cond>(value, column_key)};
            }
            case type_Float: {
                return std::unique_ptr<ParentNode>{new SizeListNode<float, Cond>(value, column_key)};
            }
            case type_Double: {
                return std::unique_ptr<ParentNode>{new SizeListNode<double, Cond>(value, column_key)};
            }
            case type_String: {
                return std::unique_ptr<ParentNode>{new SizeListNode<String, Cond>(value, column_key)};
            }
            case type_Binary: {
                return std::unique_ptr<ParentNode>{new SizeListNode<Binary, Cond>(value, column_key)};
            }
            case type_Timestamp: {
                return std::unique_ptr<ParentNode>{new SizeListNode<Timestamp, Cond>(value, column_key)};
            }
            case type_LinkList: {
                return std::unique_ptr<ParentNode>{new SizeListNode<ObjKey, Cond>(value, column_key)};
            }
            default: {
                throw_type_mismatch_error();
            }
        }
    }
    switch (type) {
        case type_String: {
            return std::unique_ptr<ParentNode>{new SizeNode<StringData, Cond>(value, column_key)};
        }
        case type_Binary: {
            return std::unique_ptr<ParentNode>{new SizeNode<BinaryData, Cond>(value, column_key)};
        }
        default: {
            throw_type_mismatch_error();
        }
    }
}

} // anonymous namespace

template <typename TConditionFunction, class T>
REALM_FORCEINLINE Query& Query::add_condition(ColKey column_key, T value)
{
    auto node = make_condition_node<TConditionFunction>(*m_table, column_key, value);
    add_node(std::move(node));
    return *this;
}


template <typename TConditionFunction>
Query& Query::add_size_condition(ColKey column_key, int64_t value)
{
    auto node = make_size_condition_node<TConditionFunction>(*m_table, column_key, value);
    add_node(std::move(node));
    return *this;
}


template <class ColumnType>
Query& Query::equal(ColKey column_key1, ColKey column_key2)
{
    auto node = std::unique_ptr<ParentNode>(new TwoColumnsNode<ColumnType, Equal>(column_key1, column_key2));
    add_node(std::move(node));
    return *this;
}

// Two column methods, any type
template <class ColumnType>
Query& Query::less(ColKey column_key1, ColKey column_key2)
{
    auto node = std::unique_ptr<ParentNode>(new TwoColumnsNode<ColumnType, Less>(column_key1, column_key2));
    add_node(std::move(node));
    return *this;
}
template <class ColumnType>
Query& Query::less_equal(ColKey column_key1, ColKey column_key2)
{
    auto node = std::unique_ptr<ParentNode>(new TwoColumnsNode<ColumnType, LessEqual>(column_key1, column_key2));
    add_node(std::move(node));
    return *this;
}
template <class ColumnType>
Query& Query::greater(ColKey column_key1, ColKey column_key2)
{
    auto node = std::unique_ptr<ParentNode>(new TwoColumnsNode<ColumnType, Greater>(column_key1, column_key2));
    add_node(std::move(node));
    return *this;
}
template <class ColumnType>
Query& Query::greater_equal(ColKey column_key1, ColKey column_key2)
{
    auto node = std::unique_ptr<ParentNode>(new TwoColumnsNode<ColumnType, GreaterEqual>(column_key1, column_key2));
    add_node(std::move(node));
    return *this;
}
template <class ColumnType>
Query& Query::not_equal(ColKey column_key1, ColKey column_key2)
{
    auto node = std::unique_ptr<ParentNode>(new TwoColumnsNode<ColumnType, NotEqual>(column_key1, column_key2));
    add_node(std::move(node));
    return *this;
}

// column vs column, integer
Query& Query::equal_int(ColKey column_key1, ColKey column_key2)
{
    return equal<ArrayInteger>(column_key1, column_key2);
}

Query& Query::not_equal_int(ColKey column_key1, ColKey column_key2)
{
    return not_equal<ArrayInteger>(column_key1, column_key2);
}

Query& Query::less_int(ColKey column_key1, ColKey column_key2)
{
    return less<ArrayInteger>(column_key1, column_key2);
}

Query& Query::greater_equal_int(ColKey column_key1, ColKey column_key2)
{
    return greater_equal<ArrayInteger>(column_key1, column_key2);
}

Query& Query::less_equal_int(ColKey column_key1, ColKey column_key2)
{
    return less_equal<ArrayInteger>(column_key1, column_key2);
}

Query& Query::greater_int(ColKey column_key1, ColKey column_key2)
{
    return greater<ArrayInteger>(column_key1, column_key2);
}


// column vs column, float
Query& Query::not_equal_float(ColKey column_key1, ColKey column_key2)
{
    return not_equal<ArrayFloat>(column_key1, column_key2);
}

Query& Query::less_float(ColKey column_key1, ColKey column_key2)
{
    return less<ArrayFloat>(column_key1, column_key2);
}

Query& Query::greater_float(ColKey column_key1, ColKey column_key2)
{
    return greater<ArrayFloat>(column_key1, column_key2);
}

Query& Query::greater_equal_float(ColKey column_key1, ColKey column_key2)
{
    return greater_equal<ArrayFloat>(column_key1, column_key2);
}

Query& Query::less_equal_float(ColKey column_key1, ColKey column_key2)
{
    return less_equal<ArrayFloat>(column_key1, column_key2);
}

Query& Query::equal_float(ColKey column_key1, ColKey column_key2)
{
    return equal<ArrayFloat>(column_key1, column_key2);
}

// column vs column, double
Query& Query::equal_double(ColKey column_key1, ColKey column_key2)
{
    return equal<ArrayDouble>(column_key1, column_key2);
}

Query& Query::less_equal_double(ColKey column_key1, ColKey column_key2)
{
    return less_equal<ArrayDouble>(column_key1, column_key2);
}

Query& Query::greater_equal_double(ColKey column_key1, ColKey column_key2)
{
    return greater_equal<ArrayDouble>(column_key1, column_key2);
}
Query& Query::greater_double(ColKey column_key1, ColKey column_key2)
{
    return greater<ArrayDouble>(column_key1, column_key2);
}
Query& Query::less_double(ColKey column_key1, ColKey column_key2)
{
    return less<ArrayDouble>(column_key1, column_key2);
}

Query& Query::not_equal_double(ColKey column_key1, ColKey column_key2)
{
    return not_equal<ArrayDouble>(column_key1, column_key2);
}

// null vs column
Query& Query::equal(ColKey column_key, null)
{
    add_condition<Equal>(column_key, null{});
    return *this;
}

Query& Query::not_equal(ColKey column_key, null)
{
    add_condition<NotEqual>(column_key, null());
    return *this;
}

// int constant vs column (we need those because '1234' is ambiguous, can convert to float/double/int64_t)
Query& Query::equal(ColKey column_key, int value)
{
    return equal(column_key, static_cast<int64_t>(value));
}
Query& Query::not_equal(ColKey column_key, int value)
{
    return not_equal(column_key, static_cast<int64_t>(value));
}
Query& Query::greater(ColKey column_key, int value)
{
    return greater(column_key, static_cast<int64_t>(value));
}
Query& Query::greater_equal(ColKey column_key, int value)
{
    return greater_equal(column_key, static_cast<int64_t>(value));
}
Query& Query::less_equal(ColKey column_key, int value)
{
    return less_equal(column_key, static_cast<int64_t>(value));
}
Query& Query::less(ColKey column_key, int value)
{
    return less(column_key, static_cast<int64_t>(value));
}
Query& Query::between(ColKey column_key, int from, int to)
{
    return between(column_key, static_cast<int64_t>(from), static_cast<int64_t>(to));
}

Query& Query::links_to(ColKey origin_column_key, ObjKey target_key)
{
    add_node(std::unique_ptr<ParentNode>(new LinksToNode(origin_column_key, target_key)));
    return *this;
}

Query& Query::links_to(ColKey origin_column, const std::vector<ObjKey>& target_keys)
{
    add_node(std::unique_ptr<ParentNode>(new LinksToNode(origin_column, target_keys)));
    return *this;
}

// int64 constant vs column
Query& Query::equal(ColKey column_key, int64_t value)
{
    add_condition<Equal>(column_key, value);
    return *this;
}
Query& Query::not_equal(ColKey column_key, int64_t value)
{
    add_condition<NotEqual>(column_key, value);
    return *this;
}
Query& Query::greater(ColKey column_key, int64_t value)
{
    add_condition<Greater>(column_key, value);
    return *this;
}
Query& Query::greater_equal(ColKey column_key, int64_t value)
{
    if (value > LLONG_MIN) {
        add_condition<Greater>(column_key, value - 1);
    }
    // field >= LLONG_MIN has no effect
    return *this;
}
Query& Query::less_equal(ColKey column_key, int64_t value)
{
    if (value < LLONG_MAX) {
        add_condition<Less>(column_key, value + 1);
    }
    // field <= LLONG_MAX has no effect
    return *this;
}
Query& Query::less(ColKey column_key, int64_t value)
{
    add_condition<Less>(column_key, value);
    return *this;
}
Query& Query::between(ColKey column_key, int64_t from, int64_t to)
{
    group();
    greater_equal(column_key, from);
    less_equal(column_key, to);
    end_group();
    return *this;
}
Query& Query::equal(ColKey column_key, bool value)
{
    add_condition<Equal>(column_key, value);
    return *this;
}
Query& Query::not_equal(ColKey column_key, bool value)
{
    add_condition<NotEqual>(column_key, value);
    return *this;
}

// ------------- float
Query& Query::equal(ColKey column_key, float value)
{
    return add_condition<Equal>(column_key, value);
}
Query& Query::not_equal(ColKey column_key, float value)
{
    return add_condition<NotEqual>(column_key, value);
}
Query& Query::greater(ColKey column_key, float value)
{
    return add_condition<Greater>(column_key, value);
}
Query& Query::greater_equal(ColKey column_key, float value)
{
    return add_condition<GreaterEqual>(column_key, value);
}
Query& Query::less_equal(ColKey column_key, float value)
{
    return add_condition<LessEqual>(column_key, value);
}
Query& Query::less(ColKey column_key, float value)
{
    return add_condition<Less>(column_key, value);
}
Query& Query::between(ColKey column_key, float from, float to)
{
    group();
    greater_equal(column_key, from);
    less_equal(column_key, to);
    end_group();
    return *this;
}


// ------------- double
Query& Query::equal(ColKey column_key, double value)
{
    return add_condition<Equal>(column_key, value);
}
Query& Query::not_equal(ColKey column_key, double value)
{
    return add_condition<NotEqual>(column_key, value);
}
Query& Query::greater(ColKey column_key, double value)
{
    return add_condition<Greater>(column_key, value);
}
Query& Query::greater_equal(ColKey column_key, double value)
{
    return add_condition<GreaterEqual>(column_key, value);
}
Query& Query::less_equal(ColKey column_key, double value)
{
    return add_condition<LessEqual>(column_key, value);
}
Query& Query::less(ColKey column_key, double value)
{
    return add_condition<Less>(column_key, value);
}
Query& Query::between(ColKey column_key, double from, double to)
{
    group();
    greater_equal(column_key, from);
    less_equal(column_key, to);
    end_group();
    return *this;
}


// ------------- Timestamp
Query& Query::greater(ColKey column_key, Timestamp value)
{
    return add_condition<Greater>(column_key, value);
}
Query& Query::equal(ColKey column_key, Timestamp value)
{
    return add_condition<Equal>(column_key, value);
}
Query& Query::not_equal(ColKey column_key, Timestamp value)
{
    return add_condition<NotEqual>(column_key, value);
}
Query& Query::greater_equal(ColKey column_key, Timestamp value)
{
    return add_condition<GreaterEqual>(column_key, value);
}
Query& Query::less_equal(ColKey column_key, Timestamp value)
{
    return add_condition<LessEqual>(column_key, value);
}
Query& Query::less(ColKey column_key, Timestamp value)
{
    return add_condition<Less>(column_key, value);
}

// ------------- ObjectId
Query& Query::greater(ColKey column_key, ObjectId value)
{
    return add_condition<Greater>(column_key, value);
}
Query& Query::equal(ColKey column_key, ObjectId value)
{
    return add_condition<Equal>(column_key, value);
}
Query& Query::not_equal(ColKey column_key, ObjectId value)
{
    return add_condition<NotEqual>(column_key, value);
}
Query& Query::greater_equal(ColKey column_key, ObjectId value)
{
    return add_condition<GreaterEqual>(column_key, value);
}
Query& Query::less_equal(ColKey column_key, ObjectId value)
{
    return add_condition<LessEqual>(column_key, value);
}
Query& Query::less(ColKey column_key, ObjectId value)
{
    return add_condition<Less>(column_key, value);
}

// ------------- Decimal128
Query& Query::greater(ColKey column_key, Decimal128 value)
{
    return add_condition<Greater>(column_key, value);
}
Query& Query::equal(ColKey column_key, Decimal128 value)
{
    return add_condition<Equal>(column_key, value);
}
Query& Query::not_equal(ColKey column_key, Decimal128 value)
{
    return add_condition<NotEqual>(column_key, value);
}
Query& Query::greater_equal(ColKey column_key, Decimal128 value)
{
    return add_condition<GreaterEqual>(column_key, value);
}
Query& Query::less_equal(ColKey column_key, Decimal128 value)
{
    return add_condition<LessEqual>(column_key, value);
}
Query& Query::less(ColKey column_key, Decimal128 value)
{
    return add_condition<Less>(column_key, value);
}
Query& Query::between(ColKey column_key, Decimal128 from, Decimal128 to)
{
    group();
    greater_equal(column_key, from);
    less_equal(column_key, to);
    end_group();
    return *this;
}

// ------------- size
Query& Query::size_equal(ColKey column_key, int64_t value)
{
    return add_size_condition<Equal>(column_key, value);
}
Query& Query::size_not_equal(ColKey column_key, int64_t value)
{
    return add_size_condition<NotEqual>(column_key, value);
}
Query& Query::size_greater(ColKey column_key, int64_t value)
{
    return add_size_condition<Greater>(column_key, value);
}
Query& Query::size_greater_equal(ColKey column_key, int64_t value)
{
    return add_size_condition<GreaterEqual>(column_key, value);
}
Query& Query::size_less_equal(ColKey column_key, int64_t value)
{
    return add_size_condition<LessEqual>(column_key, value);
}
Query& Query::size_less(ColKey column_key, int64_t value)
{
    return add_size_condition<Less>(column_key, value);
}
Query& Query::size_between(ColKey column_key, int64_t from, int64_t to)
{
    group();
    size_greater_equal(column_key, from);
    size_less_equal(column_key, to);
    end_group();
    return *this;
}

// Strings, StringData()

Query& Query::equal(ColKey column_key, StringData value, bool case_sensitive)
{
    if (case_sensitive)
        add_condition<Equal>(column_key, value);
    else
        add_condition<EqualIns>(column_key, value);
    return *this;
}
Query& Query::begins_with(ColKey column_key, StringData value, bool case_sensitive)
{
    if (case_sensitive)
        add_condition<BeginsWith>(column_key, value);
    else
        add_condition<BeginsWithIns>(column_key, value);
    return *this;
}
Query& Query::ends_with(ColKey column_key, StringData value, bool case_sensitive)
{
    if (case_sensitive)
        add_condition<EndsWith>(column_key, value);
    else
        add_condition<EndsWithIns>(column_key, value);
    return *this;
}
Query& Query::contains(ColKey column_key, StringData value, bool case_sensitive)
{
    if (case_sensitive)
        add_condition<Contains>(column_key, value);
    else
        add_condition<ContainsIns>(column_key, value);
    return *this;
}
Query& Query::not_equal(ColKey column_key, StringData value, bool case_sensitive)
{
    if (case_sensitive)
        add_condition<NotEqual>(column_key, value);
    else
        add_condition<NotEqualIns>(column_key, value);
    return *this;
}
Query& Query::like(ColKey column_key, StringData value, bool case_sensitive)
{
    if (case_sensitive)
        add_condition<Like>(column_key, value);
    else
        add_condition<LikeIns>(column_key, value);
    return *this;
}


// Aggregates =================================================================================

bool Query::eval_object(ConstObj& obj) const
{
    if (has_conditions())
        return root_node()->match(obj);

    // Query has no conditions, so all rows match, also the user given argument
    return true;
}


template <typename T>
void Query::aggregate(QueryStateBase& st, ColKey column_key, size_t* resultcount, ObjKey* return_ndx) const
{
    using LeafType = typename ColumnTypeTraits<T>::cluster_leaf_type;

    if (!has_conditions() && !m_view) {
        // use table aggregate
        // return m_table.unchecked_ptr()->aggregate<action, T, R>(column_key, T{}, resultcount, return_ndx);
        return;
    }
    else {

        // Aggregate with criteria - goes through the nodes in the query system
        init();

        if (!m_view) {
            auto pn = root_node();
            auto node = pn->m_children[find_best_node(pn)];
            if (node->has_search_index()) {
                node->index_based_aggregate(size_t(-1), [&](ConstObj& obj) -> bool {
                    if (eval_object(obj)) {
                        st.match(size_t(obj.get_key().value), obj.get<T>(column_key));
                        return true;
                    }
                    else {
                        return false;
                    }
                });
            }
            else {
                // no index, traverse cluster tree
                node = pn;
                LeafType leaf(m_table.unchecked_ptr()->get_alloc());

                auto f = [column_key, &leaf, &node, &st, this](const Cluster* cluster) {
                    size_t e = cluster->node_size();
                    node->set_cluster(cluster);
                    cluster->init_leaf(column_key, &leaf);
                    st.m_key_offset = cluster->get_offset();
                    st.m_key_values = cluster->get_key_array();
                    aggregate_internal(node, &st, 0, e, &leaf);
                    // Continue
                    return false;
                };

                m_table.unchecked_ptr()->traverse_clusters(f);
            }
        }
        else {
            for (size_t t = 0; t < m_view->size(); t++) {
                ConstObj obj = m_view->get_object(t);
                if (eval_object(obj)) {
                    st.match(size_t(obj.get_key().value), obj.get<T>(column_key));
                }
            }
        }

        if (resultcount) {
            *resultcount = st.m_match_count;
        }

        if (return_ndx) {
            *return_ndx = st.m_minmax_index;
        }
    }
}

size_t Query::find_best_node(ParentNode* pn) const
{
    auto score_compare = [](const ParentNode* a, const ParentNode* b) { return a->cost() < b->cost(); };
    size_t best = std::distance(pn->m_children.begin(),
                                std::min_element(pn->m_children.begin(), pn->m_children.end(), score_compare));
    return best;
}

/**************************************************************************************************************
*                                                                                                             *
* Main entry point of a query. Schedules calls to aggregate_local                                             *
* Return value is the result of the query, or Array pointer for FindAll.                                      *
*                                                                                                             *
**************************************************************************************************************/

void Query::aggregate_internal(ParentNode* pn, QueryStateBase* st, size_t start, size_t end,
                               ArrayPayload* source_column) const
{
    while (start < end) {
        // Executes start...end range of a query and will stay inside the condition loop of the node it was called
        // on. Can be called on any node; yields same result, but different performance. Returns prematurely if
        // condition of called node has evaluated to true local_matches number of times.
        // Return value is the next row for resuming aggregating (next row that caller must call aggregate_local on)
        size_t best = find_best_node(pn);
        start = pn->m_children[best]->aggregate_local(st, start, end, findlocals, source_column);

        // Make remaining conditions compute their m_dD (statistics)
        for (size_t c = 0; c < pn->m_children.size() && start < end; c++) {
            if (c == best)
                continue;

            // Skip test if there is no way its cost can ever be better than best node's
            double cost = pn->m_children[c]->cost();
            if (pn->m_children[c]->m_dT < cost) {

                // Limit to bestdist in order not to skip too large parts of index nodes
                size_t maxD = pn->m_children[c]->m_dT == 0.0 ? end - start : bestdist;
                size_t td = pn->m_children[c]->m_dT == 0.0 ? end : (start + maxD > end ? end : start + maxD);
                start = pn->m_children[c]->aggregate_local(st, start, td, probe_matches, source_column);
            }
        }
    }
}


// Sum

int64_t Query::sum_int(ColKey column_key) const
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_Sum);
#endif

    QueryStateSum<int64_t> st;
    if (m_table->is_nullable(column_key)) {
        aggregate<util::Optional<int64_t>>(st, column_key);
    }
    else {
        aggregate<int64_t>(st, column_key);
    }
    return st.m_state;
}
double Query::sum_float(ColKey column_key) const
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_Sum);
#endif

    QueryStateSum<double> st;
    aggregate<float>(st, column_key);
    return st.m_state;
}
double Query::sum_double(ColKey column_key) const
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_Sum);
#endif
    QueryStateSum<double> st;
    aggregate<double>(st, column_key);
    return st.m_state;
}

Decimal128 Query::sum_decimal128(ColKey column_key) const
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_Sum);
#endif

    QueryStateSum<Decimal128> st;
    aggregate<Decimal128>(st, column_key);
    return st.m_state;
}

// Maximum

int64_t Query::maximum_int(ColKey column_key, ObjKey* return_ndx) const
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_Maximum);
#endif

    QueryStateMax<int64_t> st;
    if (m_table->is_nullable(column_key)) {
        aggregate<util::Optional<int64_t>>(st, column_key, nullptr, return_ndx);
    }
    else {
        aggregate<int64_t>(st, column_key, nullptr, return_ndx);
    }
    return st.m_state;
}

float Query::maximum_float(ColKey column_key, ObjKey* return_ndx) const
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_Maximum);
#endif

    QueryStateMax<float> st;
    aggregate<float>(st, column_key, nullptr, return_ndx);
    return st.m_state;
}
double Query::maximum_double(ColKey column_key, ObjKey* return_ndx) const
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_Maximum);
#endif

    QueryStateMax<double> st;
    aggregate<double>(st, column_key, nullptr, return_ndx);
    return st.m_state;
}

Decimal128 Query::maximum_decimal128(ColKey column_key, ObjKey* return_ndx) const
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_Maximum);
#endif

    QueryStateMax<Decimal128> st;
    aggregate<Decimal128>(st, column_key, nullptr, return_ndx);
    return st.m_state;
}


// Minimum

int64_t Query::minimum_int(ColKey column_key, ObjKey* return_ndx) const
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_Minimum);
#endif

    QueryStateMin<int64_t> st;
    if (m_table->is_nullable(column_key)) {
        aggregate<util::Optional<int64_t>>(st, column_key, nullptr, return_ndx);
    }
    else {
        aggregate<int64_t>(st, column_key, nullptr, return_ndx);
    }
    return st.m_state;
}
float Query::minimum_float(ColKey column_key, ObjKey* return_ndx) const
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_Minimum);
#endif

    QueryStateMin<float> st;
    aggregate<float>(st, column_key, nullptr, return_ndx);
    return st.m_state;
}
double Query::minimum_double(ColKey column_key, ObjKey* return_ndx) const
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_Minimum);
#endif

    QueryStateMin<double> st;
    aggregate<double>(st, column_key, nullptr, return_ndx);
    return st.m_state;
}

Timestamp Query::minimum_timestamp(ColKey column_key, ObjKey* return_ndx)
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_Minimum);
#endif

    QueryStateMin<Timestamp> st;
    aggregate<Timestamp>(st, column_key, nullptr, return_ndx);
    return st.m_state;
}

Decimal128 Query::minimum_decimal128(ColKey column_key, ObjKey* return_ndx) const
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_Minimum);
#endif

    QueryStateMin<Decimal128> st;
    aggregate<Decimal128>(st, column_key, nullptr, return_ndx);
    return st.m_state;
}

Timestamp Query::maximum_timestamp(ColKey column_key, ObjKey* return_ndx)
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_Maximum);
#endif

    QueryStateMax<Timestamp> st;
    aggregate<Timestamp>(st, column_key, nullptr, return_ndx);
    return st.m_state;
}


// Average

template <typename T, bool Nullable>
double Query::average(ColKey column_key, size_t* resultcount) const
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_Average);
#endif
    using ResultType = typename AggregateResultType<T, act_Sum>::result_type;
    size_t resultcount2 = 0;
    QueryStateSum<ResultType> st;
    aggregate<T>(st, column_key, &resultcount2);
    auto sum1 = st.m_state;
    double avg1 = 0;
    if (resultcount2 != 0)
        avg1 = static_cast<double>(sum1) / resultcount2;
    if (resultcount)
        *resultcount = resultcount2;
    return avg1;
}

double Query::average_int(ColKey column_key, size_t* resultcount) const
{
    if (m_table->is_nullable(column_key)) {
        return average<util::Optional<int64_t>, true>(column_key, resultcount);
    }
    return average<int64_t, false>(column_key, resultcount);
}
double Query::average_float(ColKey column_key, size_t* resultcount) const
{
    if (m_table->is_nullable(column_key)) {
        return average<float, true>(column_key, resultcount);
    }
    return average<float, false>(column_key, resultcount);
}
double Query::average_double(ColKey column_key, size_t* resultcount) const
{
    if (m_table->is_nullable(column_key)) {
        return average<double, true>(column_key, resultcount);
    }
    return average<double, false>(column_key, resultcount);
}
Decimal128 Query::average_decimal128(ColKey column_key, size_t* resultcount) const
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_Average);
#endif
    size_t resultcount2 = 0;
    QueryStateSum<Decimal128> st;
    aggregate<Decimal128>(st, column_key, &resultcount2);
    auto sum1 = st.m_state;
    Decimal128 avg1;
    if (resultcount2 != 0)
        avg1 = sum1 / resultcount2;
    if (resultcount)
        *resultcount = resultcount2;
    return avg1;
}


// Grouping
Query& Query::group()
{
    m_groups.emplace_back();
    return *this;
}
Query& Query::end_group()
{
    if (m_groups.size() < 2) {
        error_code = "Unbalanced group";
        return *this;
    }

    auto end_root_node = std::move(m_groups.back().m_root_node);
    m_groups.pop_back();

    if (end_root_node) {
        add_node(std::move(end_root_node));
    }

    handle_pending_not();
    return *this;
}

// Not creates an implicit group to capture the term that we want to negate.
Query& Query::Not()
{
    group();
    m_groups.back().m_pending_not = true;

    return *this;
}

// And-terms must end by calling handle_pending_not. This will check if a negation is pending,
// and if so, it will end the implicit group created to hold the term to negate. Note that
// end_group itself will recurse into handle_pending_not if multiple implicit groups are nested
// within each other.
void Query::handle_pending_not()
{
    auto& current_group = m_groups.back();
    if (m_groups.size() > 1 && current_group.m_pending_not) {
        // we are inside group(s) implicitly created to handle a not, so reparent its
        // nodes into a NotNode.
        auto not_node = std::unique_ptr<ParentNode>(new NotNode(std::move(current_group.m_root_node)));
        current_group.m_pending_not = false;

        add_node(std::move(not_node));
        end_group();
    }
}

Query& Query::Or()
{
    auto& current_group = m_groups.back();
    if (current_group.m_state != QueryGroup::State::OrConditionChildren) {
        // Reparent the current group's nodes within an OrNode.
        add_node(std::unique_ptr<ParentNode>(new OrNode(std::move(current_group.m_root_node))));
    }
    current_group.m_state = QueryGroup::State::OrCondition;

    return *this;
}


ObjKey Query::find()
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_Find);
#endif

    init();

    // User created query with no criteria; return first
    if (!has_conditions()) {
        if (m_view) {
            if (m_view->size() > 0) {
                return m_view->get_key(0);
            }
            return null_key;
        }
        else
            return m_table->size() == 0 ? null_key : m_table.unchecked_ptr()->begin()->get_key();
    }

    if (m_view) {
        size_t sz = m_view->size();
        for (size_t i = 0; i < sz; i++) {
            ConstObj obj = m_view->get_object(i);
            if (eval_object(obj)) {
                return obj.get_key();
            }
        }
        return null_key;
    }
    else {
        auto node = root_node();
        ObjKey key;
        auto f = [&node, &key](const Cluster* cluster) {
            size_t end = cluster->node_size();
            node->set_cluster(cluster);
            size_t res = node->find_first(0, end);
            if (res != not_found) {
                key = cluster->get_real_key(res);
                // We should just find one - we're done
                return true;
            }
            // Continue
            return false;
        };

        m_table->traverse_clusters(f);
        return key;
    }
}

void Query::find_all(ConstTableView& ret, size_t begin, size_t end, size_t limit) const
{
    if (limit == 0)
        return;

    REALM_ASSERT_3(begin, <=, m_table->size());

    init();

    if (m_view) {
        if (end == size_t(-1))
            end = m_view->size();
        for (size_t t = begin; t < end && ret.size() < limit; t++) {
            ConstObj obj = m_view->get_object(t);
            if (eval_object(obj)) {
                ret.m_key_values.add(obj.get_key());
            }
        }
    }
    else {
        if (end == size_t(-1))
            end = m_table->size();
        if (!has_conditions()) {
            KeyColumn& refs = ret.m_key_values;

            auto f = [&begin, &end, &limit, &refs](const Cluster* cluster) {
                size_t e = cluster->node_size();
                if (begin < e) {
                    if (e > end) {
                        e = end;
                    }
                    auto offset = cluster->get_offset();
                    auto key_values = cluster->get_key_array();
                    for (size_t i = begin; (i < e) && limit; i++) {
                        refs.add(ObjKey(key_values->get(i) + offset));
                        --limit;
                    }
                    begin = 0;
                }
                else {
                    begin -= e;
                }
                end -= e;
                // Stop if end is reached
                return (end == 0) || (limit == 0);
            };

            m_table->traverse_clusters(f);
        }
        else {
            auto pn = root_node();
            auto node = pn->m_children[find_best_node(pn)];
            if (node->has_search_index()) {
                // translate begin/end limiters into corresponding keys
                auto begin_key = (begin >= m_table->size()) ? ObjKey() : m_table->get_object(begin).get_key();
                auto end_key = (end >= m_table->size()) ? ObjKey() : m_table->get_object(end).get_key();
                KeyColumn& refs = ret.m_key_values;
                node->index_based_aggregate(limit, [&](ConstObj& obj) -> bool {
                    auto key = obj.get_key();
                    if (begin_key && key < begin_key)
                        return false;
                    if (end_key && !(key < end_key))
                        return false;
                    if (eval_object(obj)) {
                        refs.add(key);
                        return true;
                    }
                    else {
                        return false;
                    }
                });
                return;
            }
            // no index on best node (and likely no index at all), descend B+-tree
            node = pn;
            QueryState<int64_t> st(act_FindAll, &ret.m_key_values, limit);

            auto f = [&begin, &end, &node, &st, this](const Cluster* cluster) {
                size_t e = cluster->node_size();
                if (begin < e) {
                    if (e > end) {
                        e = end;
                    }
                    node->set_cluster(cluster);
                    st.m_key_offset = cluster->get_offset();
                    st.m_key_values = cluster->get_key_array();
                    aggregate_internal(node, &st, begin, e, nullptr);
                    begin = 0;
                }
                else {
                    begin -= e;
                }
                end -= e;
                // Stop if limit or end is reached
                return end == 0 || st.m_match_count == st.m_limit;
            };

            m_table->traverse_clusters(f);
        }
    }
}

TableView Query::find_all(size_t start, size_t end, size_t limit)
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_FindAll);
#endif

    TableView ret(m_table, *this, start, end, limit);
    ret.do_sync();
    return ret;
}


size_t Query::do_count(size_t limit) const
{
    if (limit == 0)
        return 0;

    if (!has_conditions()) {
        // User created query with no criteria; count all
        if (m_view) {
            return std::min(m_view->size(), limit);
        }
        else {
            return std::min(m_table->size(), limit);
        }
    }

    init();
    size_t cnt = 0;

    if (m_view) {
        size_t sz = m_view->size();
        for (size_t t = 0; t < sz && cnt < limit; t++) {
            ConstObj obj = m_view->get_object(t);
            if (eval_object(obj)) {
                cnt++;
            }
        }
    }
    else {
        size_t counter = 0;
        auto pn = root_node();
        auto node = pn->m_children[find_best_node(pn)];
        if (node->has_search_index()) {
            node->index_based_aggregate(limit, [&](ConstObj& obj) -> bool {
                if (eval_object(obj)) {
                    ++counter;
                    return true;
                }
                else {
                    return false;
                }
            });
            return counter;
        }
        // no index, descend down the B+-tree instead
        node = pn;
        QueryStateCount st(limit);

        auto f = [&node, &st, this](const Cluster* cluster) {
            size_t e = cluster->node_size();
            node->set_cluster(cluster);
            st.m_key_offset = cluster->get_offset();
            st.m_key_values = cluster->get_key_array();
            aggregate_internal(node, &st, 0, e, nullptr);
            // Stop if limit or end is reached
            return st.m_match_count == st.m_limit;
        };

        m_table->traverse_clusters(f);

        cnt = st.get_count();
    }

    return cnt;
}

size_t Query::count() const
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_Count);
#endif
    return do_count();
}

TableView Query::find_all(const DescriptorOrdering& descriptor)
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_FindAll);
#endif
    if (descriptor.is_empty()) {
        return find_all();
    }

    const size_t default_start = 0;
    const size_t default_end = size_t(-1);
    const size_t default_limit = size_t(-1);

    bool only_limit = true;
    size_t min_limit = size_t(-1);
    for (size_t i = 0; i < descriptor.size(); ++i) {
        if (descriptor.get_type(i) != DescriptorType::Limit) {
            only_limit = false;
            break;
        }
        else {
            REALM_ASSERT(dynamic_cast<const LimitDescriptor*>(descriptor[i]));
            const LimitDescriptor* limit = static_cast<const LimitDescriptor*>(descriptor[i]);
            min_limit = std::min(min_limit, limit->get_limit());
        }
    }
    if (only_limit) {
        return find_all(default_start, default_end, min_limit);
    }

    TableView ret(m_table, *this, default_start, default_end, default_limit);
    ret.apply_descriptor_ordering(descriptor);
    return ret;
}

size_t Query::count(const DescriptorOrdering& descriptor)
{
#if REALM_METRICS
    std::unique_ptr<MetricTimer> metric_timer = QueryInfo::track(this, QueryInfo::type_Count);
#endif
    realm::util::Optional<size_t> min_limit = descriptor.get_min_limit();

    if (bool(min_limit) && *min_limit == 0)
        return 0;

    const size_t start = 0;
    const size_t end = m_table->size();
    size_t limit = size_t(-1);

    if (!descriptor.will_apply_distinct()) {
        if (bool(min_limit)) {
            limit = *min_limit;
        }
        return do_count(limit);
    }

    TableView ret(m_table, *this, start, end, limit);
    ret.apply_descriptor_ordering(descriptor);
    return ret.size();
}

// todo, not sure if start, end and limit could be useful for delete.
size_t Query::remove()
{
    TableView tv = find_all();
    size_t rows = tv.size();
    tv.clear();
    return rows;
}

#if REALM_MULTITHREAD_QUERY
TableView Query::find_all_multi(size_t start, size_t end)
{
    static_cast<void>(start);
    static_cast<void>(end);

    // Initialization
    init();
    ts.next_job = start;
    ts.end_job = end;
    ts.done_job = 0;
    ts.count = 0;
    ts.table = &table;
    ts.node = first[0];

    // Signal all threads to start
    pthread_mutex_unlock(&ts.jobs_mutex);
    pthread_cond_broadcast(&ts.jobs_cond);

    // Wait until all threads have completed
    pthread_mutex_lock(&ts.completed_mutex);
    while (ts.done_job < ts.end_job)
        pthread_cond_wait(&ts.completed_cond, &ts.completed_mutex);
    pthread_mutex_lock(&ts.jobs_mutex);
    pthread_mutex_unlock(&ts.completed_mutex);

    TableView tv(*m_table);

    // Sort search results because user expects ascending order
    sort(ts.chunks.begin(), ts.chunks.end(), &Query::comp);
    for (size_t i = 0; i < ts.chunks.size(); ++i) {
        const size_t from = ts.chunks[i].m_root_node;
        const size_t upto = (i == ts.chunks.size() - 1) ? size_t(-1) : ts.chunks[i + 1].m_root_node;
        size_t first = ts.chunks[i].second;

        while (first < ts.results.size() && ts.results[first] < upto && ts.results[first] >= from) {
            tv.get_ref_column().add(ts.results[first]);
            ++first;
        }
    }

    return move(tv);
}

int Query::set_threads(unsigned int threadcount)
{
#if defined(_WIN32) || defined(__WIN32__) || defined(_WIN64)
    pthread_win32_process_attach_np();
#endif
    pthread_mutex_init(&ts.result_mutex, nullptr);
    pthread_cond_init(&ts.completed_cond, nullptr);
    pthread_mutex_init(&ts.jobs_mutex, nullptr);
    pthread_mutex_init(&ts.completed_mutex, nullptr);
    pthread_cond_init(&ts.jobs_cond, nullptr);

    pthread_mutex_lock(&ts.jobs_mutex);

    for (size_t i = 0; i < m_threadcount; ++i)
        pthread_detach(threads[i]);

    for (size_t i = 0; i < threadcount; ++i) {
        int r = pthread_create(&threads[i], nullptr, query_thread, (void*)&ts);
        if (r != 0)
            REALM_ASSERT(false); // todo
    }
    m_threadcount = threadcount;
    return 0;
}


void* Query::query_thread(void* arg)
{
    static_cast<void>(arg);
    thread_state* ts = static_cast<thread_state*>(arg);

    std::vector<size_t> res;
    std::vector<pair<size_t, size_t>> chunks;

    for (;;) {
        // Main waiting loop that waits for a query to start
        pthread_mutex_lock(&ts->jobs_mutex);
        while (ts->next_job == ts->end_job)
            pthread_cond_wait(&ts->jobs_cond, &ts->jobs_mutex);
        pthread_mutex_unlock(&ts->jobs_mutex);

        for (;;) {
            // Pick a job
            pthread_mutex_lock(&ts->jobs_mutex);
            if (ts->next_job == ts->end_job)
                break;
            const size_t chunk = min(ts->end_job - ts->next_job, thread_chunk_size);
            const size_t mine = ts->next_job;
            ts->next_job += chunk;
            size_t r = mine - 1;
            const size_t end = mine + chunk;

            pthread_mutex_unlock(&ts->jobs_mutex);

            // Execute job
            for (;;) {
                r = ts->node->find_first(r + 1, end);
                if (r == end)
                    break;
                res.push_back(r);
            }

            // Append result in common queue shared by all threads.
            pthread_mutex_lock(&ts->result_mutex);
            ts->done_job += chunk;
            if (res.size() > 0) {
                ts->chunks.push_back(std::pair<size_t, size_t>(mine, ts->results.size()));
                ts->count += res.size();
                for (size_t i = 0; i < res.size(); i++) {
                    ts->results.push_back(res[i]);
                }
                res.clear();
            }
            pthread_mutex_unlock(&ts->result_mutex);

            // Signal main thread that we might have compleeted
            pthread_mutex_lock(&ts->completed_mutex);
            pthread_cond_signal(&ts->completed_cond);
            pthread_mutex_unlock(&ts->completed_mutex);
        }
    }
    return 0;
}

#endif // REALM_MULTITHREADQUERY

std::string Query::validate()
{
    if (!m_groups.size())
        return "";

    if (error_code != "") // errors detected by QueryInterface
        return error_code;

    if (!root_node())
        return "Syntax error";

    return root_node()->validate(); // errors detected by QueryEngine
}

std::string Query::get_description(util::serializer::SerialisationState& state) const
{
    if (root_node()) {
        if (m_view) {
            throw SerialisationError("Serialisation of a query constrianed by a view is not currently supported");
        }
        return root_node()->describe_expression(state);
    }
    // An empty query returns all results and one way to indicate this
    // is to serialise TRUEPREDICATE which is functionally equivilent
    return "TRUEPREDICATE";
}

std::string Query::get_description() const
{
    util::serializer::SerialisationState state;
    return get_description(state);
}

void Query::init() const
{
    m_table.check();
    if (ParentNode* root = root_node()) {
        root->init();
        std::vector<ParentNode*> vec;
        root->gather_children(vec);
    }
}

size_t Query::find_internal(size_t start, size_t end) const
{
    if (end == size_t(-1))
        end = m_table.unchecked_ptr()->size();
    if (start == end)
        return not_found;

    size_t r;
    if (ParentNode* root = root_node())
        r = root->find_first(start, end);
    else
        r = start; // user built an empty query; return any first

    if (r == m_table.unchecked_ptr()->size())
        return not_found;
    else
        return r;
}

void Query::add_node(std::unique_ptr<ParentNode> node)
{
    REALM_ASSERT(node);
    using State = QueryGroup::State;

    if (m_table)
        node->set_table(m_table);

    auto& current_group = m_groups.back();
    switch (current_group.m_state) {
        case QueryGroup::State::OrCondition: {
            REALM_ASSERT_DEBUG(dynamic_cast<OrNode*>(current_group.m_root_node.get()));
            OrNode* or_node = static_cast<OrNode*>(current_group.m_root_node.get());
            or_node->m_conditions.emplace_back(std::move(node));
            current_group.m_state = State::OrConditionChildren;
            break;
        }
        case QueryGroup::State::OrConditionChildren: {
            REALM_ASSERT_DEBUG(dynamic_cast<OrNode*>(current_group.m_root_node.get()));
            OrNode* or_node = static_cast<OrNode*>(current_group.m_root_node.get());
            or_node->m_conditions.back()->add_child(std::move(node));
            break;
        }
        default: {
            if (!current_group.m_root_node) {
                current_group.m_root_node = std::move(node);
            }
            else {
                current_group.m_root_node->add_child(std::move(node));
            }
        }
    }

    handle_pending_not();
}

/* ********************************************************************************************************************
*
*  Stuff related to next-generation query syntax
*
********************************************************************************************************************
*/

Query& Query::and_query(const Query& q)
{
    Query copy(q);
    return and_query(std::move(copy));
}

Query& Query::and_query(Query&& q)
{
    if (q.root_node()) {
        add_node(std::move(q.m_groups[0].m_root_node));

        if (q.m_source_link_list) {
            REALM_ASSERT(!m_source_link_list || *m_source_link_list == *q.m_source_link_list);
            m_source_link_list = std::move(q.m_source_link_list);
            m_view = m_source_link_list.get();
        }
    }

    return *this;
}

Query Query::operator||(const Query& q)
{
    Query q2(m_table);
    q2.and_query(*this);
    q2.Or();
    q2.and_query(q);

    return q2;
}


Query Query::operator&&(const Query& q)
{
    if (!root_node())
        return q;

    if (!q.root_node())
        return *this;

    Query q2(m_table);
    q2.and_query(*this);
    q2.and_query(q);

    return q2;
}


Query Query::operator!()
{
    if (!root_node())
        throw util::runtime_error("negation of empty query is not supported");
    Query q(m_table);
    q.Not();
    q.and_query(*this);
    return q;
}

void Query::get_outside_versions(TableVersions& versions) const
{
    if (m_table) {
        if (m_table_keys.empty()) {
            // Store primary table info
            m_table_keys.push_back(m_table.unchecked_ptr()->get_key());

            if (ParentNode* root = root_node())
                root->get_link_dependencies(m_table_keys);
        }
        versions.emplace_back(m_table.unchecked_ptr()->get_key(), m_table.unchecked_ptr()->get_content_version());

        if (Group* g = m_table.unchecked_ptr()->get_parent_group()) {
            // update table versions for linked tables - first entry is primary table - skip it
            auto end = m_table_keys.end();
            auto it = m_table_keys.begin() + 1;
            while (it != end) {
                versions.emplace_back(*it, g->get_table(*it)->get_content_version());
                ++it;
            }
        }
        if (m_view) {
            m_view->get_dependencies(versions);
        }
    }
}

TableVersions Query::sync_view_if_needed() const
{
    if (m_view) {
        m_view->sync_if_needed();
    }
    TableVersions ret;
    get_outside_versions(ret);
    return ret;
}

QueryGroup::QueryGroup(const QueryGroup& other)
    : m_root_node(other.m_root_node ? other.m_root_node->clone() : nullptr)
    , m_pending_not(other.m_pending_not)
    , m_state(other.m_state)
{
}

QueryGroup& QueryGroup::operator=(const QueryGroup& other)
{
    if (this != &other) {
        m_root_node = other.m_root_node ? other.m_root_node->clone() : nullptr;
        m_pending_not = other.m_pending_not;
    }
    return *this;
}

