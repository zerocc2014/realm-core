%skeleton "lalr1.cc" /* -*- C++ -*- */
%require "3.4"
%defines
// %no-lines

%define api.token.constructor
%define api.value.type variant
%define parse.assert

%code requires {
  # include <string>
  # include "realm/query_expression.hpp"
  namespace realm {
  class ParserDriver;
  }
  using namespace realm;
}

// The parsing context.
%param { realm::ParserDriver& drv }

%locations

%define parse.trace
%define parse.error verbose

%code {
#include "realm/query/driver.hpp"
}

%define api.token.prefix {TOK_}
%token
  END  0  "end of file"
  TRUEPREDICATE "true"
  ASSIGN  ":="
  EQUALS  "=="
  LESS    "<"
  GREATER ">"
  GREATER_EQUAL ">="
  LESS_EQUAL    "<="
  AND     "&&"
  OR      "||"
  MINUS   "-"
  PLUS    "+"
  STAR    "*"
  SLASH   "/"
  LPAREN  "("
  RPAREN  ")"
  DOT     "."
;

%token <std::string> IDENTIFIER "identifier"
%token <std::string> STRING "string"
%token <int64_t> NUMBER "number"
%token <double> FLOAT "float"
%type  <std::unique_ptr<realm::Subexpr>> exp
%type  <realm::Query> pred
%type path
%type path_elem

%printer { util::serializer::SerialisationState state; yyo << $$->description(state); } <std::unique_ptr<realm::Subexpr>>;
%printer { yyo << $$.get_description(); } <realm::Query>;
%printer { yyo << $$; } <*>;
%printer { yyo << "<>"; } <>;

%%
%start unit;
unit: pred  {
  drv.result = $1;
};

%left "||";
%left "&&";
%left "+" "-";
%left "*" "/";

exp:
  NUMBER            { $$.reset(new realm::Value<int64_t>($1)); }
| STRING            { $$.reset(new ConstantStringValue($1)); }
| FLOAT             { $$.reset(new realm::Value<double>($1)); }
| path IDENTIFIER   { $$.reset(drv.link_chain.column($2)); }
| "(" exp ")"       { $$ = std::move($2); }

pred:
  exp "==" exp      {
                        std::unique_ptr<realm::Subexpr> l;
                        std::unique_ptr<realm::Subexpr> r;
                        if ($1->has_constant_evaluation()) {
                            l = std::move($1);
                            r = std::move($3);
                        }
                        else {
                            l = std::move($3);
                            r = std::move($1);
                        }
                        auto type = l->get_type();
                        switch (type) {
                        case type_Int:
                            $$ = Query(std::unique_ptr<Expression>(new Compare<Equal, Int>(std::move(l), std::move(r))));
                            break;
                        case type_String:
                            $$ = Query(std::unique_ptr<Expression>(new Compare<Equal, String>(std::move(l), std::move(r))));
                            break;
                        default:
                            break;
                        }
                    }
| exp "<" exp       { 
                        auto type = $1->get_type();
                        switch (type) {
                        case type_Int:
                            $$ = Query(std::unique_ptr<Expression>(new Compare<Less, Int>(std::move($1), std::move($3))));
                            break;
                        case type_String:
                            $$ = Query(std::unique_ptr<Expression>(new Compare<Less, String>(std::move($1), std::move($3))));
                            break;
                        default:
                            break;
                        }
                    }
| exp ">" exp       { 
                        auto type = $1->get_type();
                        switch (type) {
                        case type_Int:
                            $$ = Query(std::unique_ptr<Expression>(new Compare<Greater, Int>(std::move($1), std::move($3))));
                            break;
                        case type_String:
                            $$ = Query(std::unique_ptr<Expression>(new Compare<Greater, String>(std::move($1), std::move($3))));
                            break;
                        default:
                            break;
                        }
                    }
| exp "<=" exp      { 
                        auto type = $1->get_type();
                        switch (type) {
                        case type_Int:
                            $$ = Query(std::unique_ptr<Expression>(new Compare<LessEqual, Int>(std::move($1), std::move($3))));
                            break;
                        case type_String:
                            $$ = Query(std::unique_ptr<Expression>(new Compare<LessEqual, String>(std::move($1), std::move($3))));
                            break;
                        default:
                            break;
                        }
                    }
| exp ">=" exp      { 
                        auto type = $1->get_type();
                        switch (type) {
                        case type_Int:
                            $$ = Query(std::unique_ptr<Expression>(new Compare<GreaterEqual, Int>(std::move($1), std::move($3))));
                            break;
                        case type_String:
                            $$ = Query(std::unique_ptr<Expression>(new Compare<GreaterEqual, String>(std::move($1), std::move($3))));
                            break;
                        default:
                            break;
                        }
                    }
| pred "&&" pred    {
                        $$ = $1 && $3;
                    }
| pred "||" pred    {
                        $$ = $1 || $3;
                    }
| "(" pred ")"      { $$ = std::move($2); }
| TRUEPREDICATE     { $$ = drv.link_chain.get_base_table()->where(); }
path:
  %empty            {}
| path path_elem    {}  

path_elem:
  IDENTIFIER "."    { drv.link_chain.link($1); }
%%

void
yy::parser::error (const location_type& l, const std::string& m)
{
    std::ostringstream ostr;
    ostr << l << ": " << m;
    drv.error(ostr.str());
}
