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

#include <realm/query_engine.hpp>

#include <realm/query_expression.hpp>
#include <realm/index_string.hpp>
#include <realm/db.hpp>
#include <realm/utilities.hpp>

using namespace realm;

ParentNode::ParentNode(const ParentNode& from)
    : m_child(from.m_child ? from.m_child->clone() : nullptr)
    , m_condition_column_name(from.m_condition_column_name)
    , m_condition_column_key(from.m_condition_column_key)
    , m_dD(from.m_dD)
    , m_dT(from.m_dT)
    , m_probes(from.m_probes)
    , m_matches(from.m_matches)
    , m_table(from.m_table)
{
}


size_t ParentNode::find_first(size_t start, size_t end)
{
    size_t sz = m_children.size();
    size_t current_cond = 0;
    size_t nb_cond_to_test = sz;

    while (REALM_LIKELY(start < end)) {
        size_t m = m_children[current_cond]->find_first_local(start, end);

        if (m != start) {
            // Pointer advanced - we will have to check all other conditions
            nb_cond_to_test = sz;
            start = m;
        }

        nb_cond_to_test--;

        // Optimized for one condition where this will be true first time
        if (REALM_LIKELY(nb_cond_to_test == 0))
            return m;

        current_cond++;

        if (current_cond == sz)
            current_cond = 0;
    }
    return not_found;
}

bool ParentNode::match(ConstObj& obj)
{
    auto cb = [this](const Cluster* cluster, size_t row) {
        set_cluster(cluster);
        size_t m = find_first(row, row + 1);
        return m != npos;
    };
    return obj.evaluate(cb);
}

size_t ParentNode::aggregate_local(QueryStateBase* st, size_t start, size_t end, size_t local_limit,
                                   ArrayPayload* source_column)
{
    // aggregate called on non-integer column type. Speed of this function is not as critical as speed of the
    // integer version, because find_first_local() is relatively slower here (because it's non-integers).
    //
    // Todo: Two speedups are possible. Simple: Initially test if there are no sub criterias and run
    // find_first_local()
    // in a tight loop if so (instead of testing if there are sub criterias after each match). Harder: Specialize
    // data type array to make array call match() directly on each match, like for integers.

    m_state = st;
    size_t local_matches = 0;

    size_t r = start - 1;
    for (;;) {
        if (local_matches == local_limit) {
            m_dD = double(r - start) / (local_matches + 1.1);
            return r + 1;
        }

        // Find first match in this condition node
        r = find_first_local(r + 1, end);
        if (r == not_found) {
            m_dD = double(r - start) / (local_matches + 1.1);
            return end;
        }

        local_matches++;

        // Find first match in remaining condition nodes
        size_t m = r;

        for (size_t c = 1; c < m_children.size(); c++) {
            m = m_children[c]->find_first_local(r, r + 1);
            if (m != r) {
                break;
            }
        }

        // If index of first match in this node equals index of first match in all remaining nodes, we have a final
        // match
        if (m == r) {
            Mixed val;
            if (source_column) {
                val = source_column->get_any(r);
            }
            bool cont = st->match(r, val);
            if (!cont) {
                return static_cast<size_t>(-1);
            }
        }
    }
}

void StringNodeEqualBase::init()
{
    m_dD = 10.0;
    StringNodeBase::init();

    if (m_is_string_enum) {
        m_dT = 1.0;
    }
    else if (m_has_search_index) {
        m_dT = 0.0;
    }
    else {
        m_dT = 10.0;
    }

    if (m_has_search_index) {
        // Will set m_index_matches, m_index_matches_destroy, m_results_start and m_results_end
        _search_index_init();
    }
}

size_t StringNodeEqualBase::find_first_local(size_t start, size_t end)
{
    REALM_ASSERT(m_table);

    if (m_has_search_index) {
        if (start < end) {
            ObjKey first_key = m_cluster->get_real_key(start);
            if (first_key < m_last_start_key) {
                // We are not advancing through the clusters. We basically don't know where we are,
                // so just start over from the beginning.
                m_results_ndx = m_results_start;
                m_actual_key = get_key(m_results_ndx);
            }
            m_last_start_key = first_key;

            // Check if we can expect to find more keys
            if (m_results_ndx < m_results_end) {
                // Check if we should advance to next key to search for
                while (first_key > m_actual_key) {
                    m_results_ndx++;
                    if (m_results_ndx == m_results_end) {
                        return not_found;
                    }
                    m_actual_key = get_key(m_results_ndx);
                }

                // If actual_key is bigger than last key, it is not in this leaf
                ObjKey last_key = m_cluster->get_real_key(end - 1);
                if (m_actual_key > last_key)
                    return not_found;

                // Now actual_key must be found in leaf keys
                return m_cluster->lower_bound_key(ObjKey(m_actual_key.value - m_cluster->get_offset()));
            }
        }
        return not_found;
    }

    return _find_first_local(start, end);
}


namespace realm {

void StringNode<Equal>::_search_index_init()
{
    FindRes fr;
    InternalFindResult res;

    m_last_start_key = ObjKey();
    m_results_start = 0;
    if (ParentNode::m_table->get_primary_key_column() == ParentNode::m_condition_column_key) {
        m_actual_key = ParentNode::m_table.unchecked_ptr()->find_first(ParentNode::m_condition_column_key,
                                                                       StringData(StringNodeBase::m_value));
        m_results_end = m_actual_key ? 1 : 0;
    }
    else {
        auto index = ParentNode::m_table.unchecked_ptr()->get_search_index(ParentNode::m_condition_column_key);
        fr = index->find_all_no_copy(StringData(StringNodeBase::m_value), res);

        switch (fr) {
            case FindRes_single:
                m_actual_key = ObjKey(res.payload);
                m_results_end = 1;
                break;
            case FindRes_column:
                m_index_matches.reset(
                    new IntegerColumn(m_table.unchecked_ptr()->get_alloc(), ref_type(res.payload))); // Throws
                m_results_start = res.start_ndx;
                m_results_end = res.end_ndx;
                m_actual_key = ObjKey(m_index_matches->get(m_results_start));
                break;
            case FindRes_not_found:
                m_index_matches.reset();
                m_results_end = 0;
                break;
        }
    }
    m_results_ndx = m_results_start;
}

void StringNode<Equal>::consume_condition(StringNode<Equal>* other)
{
    // If a search index is present, don't try to combine conditions since index search is most likely faster.
    // Assuming N elements to search and M conditions to check:
    // 1) search index present:                     O(log(N)*M)
    // 2) no search index, combine conditions:      O(N)
    // 3) no search index, conditions not combined: O(N*M)
    // In practice N is much larger than M, so if we have a search index, choose 1, otherwise if possible choose 2.
    REALM_ASSERT(m_condition_column_key == other->m_condition_column_key);
    REALM_ASSERT(other->m_needles.empty());
    if (m_needles.empty()) {
        m_needles.insert(bool(m_value) ? StringData(*m_value) : StringData());
    }
    if (bool(other->m_value)) {
        m_needle_storage.push_back(util::StringBuffer());
        m_needle_storage.back().append(*other->m_value);
        m_needles.insert(StringData(m_needle_storage.back().data(), m_needle_storage.back().size()));
    }
    else {
        m_needles.insert(StringData());
    }
}

size_t StringNode<Equal>::_find_first_local(size_t start, size_t end)
{
    if (m_needles.empty()) {
        return m_leaf_ptr->find_first(m_value, start, end);
    }
    else {
        size_t n = m_leaf_ptr->size();
        if (end == npos)
            end = n;
        REALM_ASSERT_7(start, <=, n, &&, end, <=, n);
        REALM_ASSERT_3(start, <=, end);

        const auto not_in_set = m_needles.end();
        // For a small number of conditions it is faster to cycle through
        // and check them individually. The threshold depends on how fast
        // our hashing of StringData is (see `StringData.hash()`). The
        // number 20 was found empirically when testing small strings
        // with N==100k
        if (m_needles.size() < 20) {
            for (size_t i = start; i < end; ++i) {
                auto element = m_leaf_ptr->get(i);
                StringData value_2{element.data(), element.size()};
                for (auto it = m_needles.begin(); it != not_in_set; ++it) {
                    if (*it == value_2)
                        return i;
                }
            }
        }
        else {
            for (size_t i = start; i < end; ++i) {
                auto element = m_leaf_ptr->get(i);
                StringData value_2{element.data(), element.size()};
                if (m_needles.find(value_2) != not_in_set)
                    return i;
            }
        }
    }
    return not_found;
}

std::string StringNode<Equal>::describe(util::serializer::SerialisationState& state) const
{
    if (m_needles.empty()) {
        return StringNodeEqualBase::describe(state);
    }

    // FIXME: once the parser supports it, print something like "column IN {s1, s2, s3}"
    std::string desc;
    bool is_first = true;
    for (auto it : m_needles) {
        StringData sd(it.data(), it.size());
        desc += (is_first ? "" : " or ") + state.describe_column(ParentNode::m_table, m_condition_column_key) + " " +
                Equal::description() + " " + util::serializer::print_value(sd);
        is_first = false;
    }
    if (!is_first) {
        desc = "(" + desc + ")";
    }
    return desc;
}


void StringNode<EqualIns>::_search_index_init()
{
    auto index = ParentNode::m_table->get_search_index(ParentNode::m_condition_column_key);
    index->find_all(m_index_matches, StringData(StringNodeBase::m_value), true);
    m_results_start = 0;
    m_results_ndx = 0;
    m_results_end = m_index_matches.size();
    if (m_results_start != m_results_end) {
        m_actual_key = m_index_matches[0];
    }
}

size_t StringNode<EqualIns>::_find_first_local(size_t start, size_t end)
{
    EqualIns cond;
    for (size_t s = start; s < end; ++s) {
        StringData t = get_string(s);

        if (cond(StringData(m_value), m_ucase.c_str(), m_lcase.c_str(), t))
            return s;
    }

    return not_found;
}

} // namespace realm

size_t NotNode::find_first_local(size_t start, size_t end)
{
    if (start <= m_known_range_start && end >= m_known_range_end) {
        return find_first_covers_known(start, end);
    }
    else if (start >= m_known_range_start && end <= m_known_range_end) {
        return find_first_covered_by_known(start, end);
    }
    else if (start < m_known_range_start && end >= m_known_range_start) {
        return find_first_overlap_lower(start, end);
    }
    else if (start <= m_known_range_end && end > m_known_range_end) {
        return find_first_overlap_upper(start, end);
    }
    else { // start > m_known_range_end || end < m_known_range_start
        return find_first_no_overlap(start, end);
    }
}

bool NotNode::evaluate_at(size_t rowndx)
{
    return m_condition->find_first(rowndx, rowndx + 1) == not_found;
}

void NotNode::update_known(size_t start, size_t end, size_t first)
{
    m_known_range_start = start;
    m_known_range_end = end;
    m_first_in_known_range = first;
}

size_t NotNode::find_first_loop(size_t start, size_t end)
{
    for (size_t i = start; i < end; ++i) {
        if (evaluate_at(i)) {
            return i;
        }
    }
    return not_found;
}

size_t NotNode::find_first_covers_known(size_t start, size_t end)
{
    // CASE: start-end covers the known range
    // [    ######    ]
    REALM_ASSERT_DEBUG(start <= m_known_range_start && end >= m_known_range_end);
    size_t result = find_first_loop(start, m_known_range_start);
    if (result != not_found) {
        update_known(start, m_known_range_end, result);
    }
    else {
        if (m_first_in_known_range != not_found) {
            update_known(start, m_known_range_end, m_first_in_known_range);
            result = m_first_in_known_range;
        }
        else {
            result = find_first_loop(m_known_range_end, end);
            update_known(start, end, result);
        }
    }
    return result;
}

size_t NotNode::find_first_covered_by_known(size_t start, size_t end)
{
    REALM_ASSERT_DEBUG(start >= m_known_range_start && end <= m_known_range_end);
    // CASE: the known range covers start-end
    // ###[#####]###
    if (m_first_in_known_range != not_found) {
        if (m_first_in_known_range > end) {
            return not_found;
        }
        else if (m_first_in_known_range >= start) {
            return m_first_in_known_range;
        }
    }
    // The first known match is before start, so we can't use the results to improve
    // heuristics.
    return find_first_loop(start, end);
}

size_t NotNode::find_first_overlap_lower(size_t start, size_t end)
{
    REALM_ASSERT_DEBUG(start < m_known_range_start && end >= m_known_range_start && end <= m_known_range_end);
    static_cast<void>(end);
    // CASE: partial overlap, lower end
    // [   ###]#####
    size_t result;
    result = find_first_loop(start, m_known_range_start);
    if (result == not_found) {
        result = m_first_in_known_range;
    }
    update_known(start, m_known_range_end, result);
    return result < end ? result : not_found;
}

size_t NotNode::find_first_overlap_upper(size_t start, size_t end)
{
    REALM_ASSERT_DEBUG(start <= m_known_range_end && start >= m_known_range_start && end > m_known_range_end);
    // CASE: partial overlap, upper end
    // ####[###    ]
    size_t result;
    if (m_first_in_known_range != not_found) {
        if (m_first_in_known_range >= start) {
            result = m_first_in_known_range;
            update_known(m_known_range_start, end, result);
        }
        else {
            result = find_first_loop(start, end);
            update_known(m_known_range_start, end, m_first_in_known_range);
        }
    }
    else {
        result = find_first_loop(m_known_range_end, end);
        update_known(m_known_range_start, end, result);
    }
    return result;
}

size_t NotNode::find_first_no_overlap(size_t start, size_t end)
{
    REALM_ASSERT_DEBUG((start < m_known_range_start && end < m_known_range_start) ||
                       (start > m_known_range_end && end > m_known_range_end));
    // CASE: no overlap
    // ### [    ]   or    [    ] ####
    // if input is a larger range, discard and replace with results.
    size_t result = find_first_loop(start, end);
    if (end - start > m_known_range_end - m_known_range_start) {
        update_known(start, end, result);
    }
    return result;
}

ExpressionNode::ExpressionNode(std::unique_ptr<Expression> expression)
: m_expression(std::move(expression))
{
    m_dD = 100.0;
    m_dT = 50.0;
}

void ExpressionNode::table_changed()
{
    m_expression->set_base_table(m_table);
}

void ExpressionNode::cluster_changed()
{
    m_expression->set_cluster(m_cluster);
}

void ExpressionNode::init()
{
    ParentNode::init();
    m_dT = m_expression->init();
}

std::string ExpressionNode::describe(util::serializer::SerialisationState& state) const
{
    if (m_expression) {
        return m_expression->description(state);
    }
    else {
        return "empty expression";
    }
}

void ExpressionNode::collect_dependencies(std::vector<TableKey>& tables) const
{
    m_expression->collect_dependencies(tables);
}

size_t ExpressionNode::find_first_local(size_t start, size_t end)
{
    return m_expression->find_first(start, end);
}

std::unique_ptr<ParentNode> ExpressionNode::clone() const
{
    return std::unique_ptr<ParentNode>(new ExpressionNode(*this));
}

ExpressionNode::ExpressionNode(const ExpressionNode& from)
    : ParentNode(from)
    , m_expression(from.m_expression->clone())
{
}
