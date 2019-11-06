#include "realm/query/driver.hpp"
#include "realm/query/query_bison.hpp"

using namespace realm;

int ParserDriver::parse(const std::string& str)
{
    parse_string = str;
    std::string dummy;
    location.initialize(&dummy);
    scan_begin();
    yy::parser parse(*this);
    parse.set_debug_level(trace_parsing);
    int res = parse();
    scan_end();
    if (parse_error) {
        throw std::runtime_error(error_string);
    }
    return res;
}

Subexpr* LinkChain::column(std::string col)
{
    auto col_key = m_current_table->get_column_key(col);
    if (!col_key) {
        std::string err = m_current_table->get_name();
        err += " has no property: ";
        err += col;
        throw std::runtime_error(err);
    }

    if (m_current_table->is_list(col_key)) {
        switch (col_key.get_type()) {
        case col_type_Int:
            return new Columns<Lst<Int>>(col_key, m_base_table, m_link_cols);
        case col_type_String:
            return new Columns<Lst<String>>(col_key, m_base_table, m_link_cols);
        default:
            break;
        }
    }
    else {
        switch (col_key.get_type()) {
        case col_type_Int:
            return new Columns<Int>(col_key, m_base_table, m_link_cols);
        case col_type_String:
            return new Columns<String>(col_key, m_base_table, m_link_cols);
        default:
            break;
        }
    }
    return nullptr;
}

Query Table::query(std::string str) const
{
    ParserDriver driver(this);
    driver.parse(str);
    return std::move(driver.result);
}
