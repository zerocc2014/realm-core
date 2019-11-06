%{ /* -*- C++ -*- */
# include <cerrno>
# include <climits>
# include <cstdlib>
# include <cstring> // strerror
# include <string>
# include "realm/query/driver.hpp"
# include "realm/query/query_bison.hpp"
%}

%option noyywrap nounput noinput batch debug

%{
  // A number symbol corresponding to the value in S.
  yy::parser::symbol_type make_STRING (const std::string &s, const yy::parser::location_type& loc);
  yy::parser::symbol_type make_NUMBER (const std::string &s, const yy::parser::location_type& loc);
  yy::parser::symbol_type make_FLOAT (const std::string &s, const yy::parser::location_type& loc);
%}

id      [a-zA-Z][a-zA-Z_0-9]*
string  ["][a-zA-Z0-9_\\/\.,: \[\]+$={}<>!();-]*["]
digit   [0-9]
int     {digit}+
float   {digit}+[.]{digit}*
blank   [ \t\r]

%{
  // Code run each time a pattern is matched.
  # define YY_USER_ACTION  loc.columns (yyleng);
%}
%%
%{
  // A handy shortcut to the location held by the driver.
  yy::location& loc = drv.location;
  // Code run each time yylex is called.
  loc.step ();
%}
{blank}+   loc.step ();
\n+        loc.lines (yyleng); loc.step ();

"=="       return yy::parser::make_EQUALS (loc);
"<"        return yy::parser::make_LESS   (loc);
">"        return yy::parser::make_GREATER(loc);
"<="       return yy::parser::make_LESS_EQUAL (loc);
">="       return yy::parser::make_GREATER_EQUAL (loc);
"&&"       return yy::parser::make_AND    (loc);
"and"      return yy::parser::make_AND    (loc);
"||"       return yy::parser::make_OR     (loc);
"or"       return yy::parser::make_OR     (loc);
"-"        return yy::parser::make_MINUS  (loc);
"+"        return yy::parser::make_PLUS   (loc);
"*"        return yy::parser::make_STAR   (loc);
"/"        return yy::parser::make_SLASH  (loc);
"("        return yy::parser::make_LPAREN (loc);
")"        return yy::parser::make_RPAREN (loc);
":="       return yy::parser::make_ASSIGN (loc);
"!"        return yy::parser::make_NOT    (loc);
"."        return yy::parser::make_DOT    (loc);
"TRUEPREDICATE" return yy::parser::make_TRUEPREDICATE (loc); 
"FALSEPREDICATE" return yy::parser::make_FALSEPREDICATE (loc); 
{int}      return make_NUMBER (yytext, loc);
{float}    return make_FLOAT (yytext, loc);
{string}   return make_STRING (yytext, loc);
{id}       return yy::parser::make_IDENTIFIER (yytext, loc);
.          {
             throw yy::parser::syntax_error
               (loc, "invalid character: " + std::string(yytext));
           }
<<EOF>>    return yy::parser::make_END (loc);
%%

yy::parser::symbol_type
make_NUMBER (const std::string &s, const yy::parser::location_type& loc)
{
  errno = 0;
  long n = strtol(s.c_str(), nullptr, 10);
  return yy::parser::make_NUMBER (int64_t(n), loc);
}

yy::parser::symbol_type
make_FLOAT (const std::string &s, const yy::parser::location_type& loc)
{
  errno = 0;
  double n = std::stod(s.c_str(), nullptr);
  return yy::parser::make_FLOAT (n, loc);
}

yy::parser::symbol_type
make_STRING (const std::string &s, const yy::parser::location_type& loc)
{
  std::string str = s.substr(1, s.size() - 2);
  return yy::parser::symbol_type ( yy::parser::token::TOK_STRING, std::move(str), std::move(loc));
}

void realm::ParserDriver::scan_begin ()
{
    yy_flex_debug = trace_scanning;
    YY_BUFFER_STATE bp;
    bp = yy_scan_bytes(parse_string.c_str(), parse_string.size());
    yy_switch_to_buffer(bp);
    scan_buffer = (void *)bp;
}

void realm::ParserDriver::scan_end ()
{
   yy_delete_buffer((YY_BUFFER_STATE)scan_buffer);
}
