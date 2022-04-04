use crate::parser::*;
use crate::process::ProcessStatement;
use crate::split::SplitStatement;

#[derive(PartialEq, Debug, Clone)]
pub enum BooleanOp {
    GreaterThan,
    GreaterThanOrEqualTo,
    LessThan,
    LessThanOrEqualTo,
    Equal,
    NotEqual,
    And,
    Or,
    Not,
}

#[derive(PartialEq, Debug, Clone)]
pub enum ArithmeticOp {
    Plus,
    Minus,
    Multiply,
    Divide,
    Mod,
}

#[derive(PartialEq, Debug, Clone)]
pub enum GroupByAttr {
    Column(String),
    Bin {
        column: String,
        size: u64,
        alias: String,
    },
}

#[derive(PartialEq, Debug, Clone)]
pub enum AstNode {
    Select {
        exprs: Vec<AstNode>,
        from: Box<AstNode>,
        qualifiers: Vec<AstNode>,
        // TODO add window
    },
    Intersect {
        tables: Vec<AstNode>,
        on: Box<AstNode>,
    },
    Union {
        tables: Vec<AstNode>,
        on: Box<AstNode>,
    },
    Equijoin {
        tables: Vec<AstNode>,
        on: Box<AstNode>,
    },
    Aggregation {
        function: String,
        // inner: Box<AstNode>,
        inner: String,
        range: Option<(f64, f64)>,
        alias: String,
    },
    UserF {
        function: String,
        column: String,
        range: Option<(f64, f64)>,
        alias: String,
    },
    AliasExpr {
        lhs: Box<AstNode>,
        op: ArithmeticOp,
        rhs: Box<AstNode>,
        alias: String,
    },
    GroupBy {
        // TODO make string or special? (e.g. day(chunk))
        attrs: Vec<GroupByAttr>,
    },
    GroupByWithKeys {
        attr: String,
        keys: Vec<String>,
    },
    Limit {
        rows: u64,
    },
    Where {
        predicate: Box<AstNode>,
    },
    Predicate {
        lhs: Box<AstNode>,
        op: BooleanOp,
        rhs: Box<AstNode>,
    },
    TableNames(Vec<String>),
    Tables(Vec<AstNode>),
    Table(SplitStatement, ProcessStatement),
    Column(String),
    Value(f64),
    DurationMs(u64),
}

pub fn build_agg_ast(pair: pest::iterators::Pair<Rule>) -> AstNode {
    match pair.as_rule() {
        Rule::aggregation_stmt => {
            let mut pairs = pair.into_inner();
            let exprs: Vec<AstNode> = pairs
                .next()
                .unwrap()
                .into_inner()
                .map(build_agg_ast)
                .collect();
            let table = build_agg_ast(pairs.next().unwrap());
            let qualifiers = pairs
                .into_iter()
                .map(|pair| build_agg_ast(pair.into_inner().next().unwrap()))
                .collect();
            AstNode::Select {
                exprs,
                from: Box::new(table),
                qualifiers,
            }
        }
        Rule::table => {
            let pair = pair.into_inner().next().unwrap();
            match pair.as_rule() {
                Rule::table_list => {
                    AstNode::TableNames(pair.into_inner().map(|t| t.as_str().to_string()).collect())
                }
                _ => build_agg_ast(pair),
            }
        }
        Rule::table_intersect => {
            let mut pairs = pair.into_inner().rev();
            let on = Box::new(build_agg_ast(pairs.next().unwrap()));
            let tables = pairs.map(build_agg_ast).collect();
            AstNode::Intersect { tables, on }
        }
        Rule::table_union => {
            let mut pairs = pair.into_inner().rev();
            let on = Box::new(build_agg_ast(pairs.next().unwrap()));
            let tables = pairs.map(build_agg_ast).collect();
            AstNode::Union { tables, on }
        }
        Rule::table_equijoin => {
            let mut pairs = pair.into_inner().rev();
            let on = Box::new(build_agg_ast(pairs.next().unwrap()));
            let tables = pairs.map(build_agg_ast).collect();
            AstNode::Equijoin { tables, on }
        }
        Rule::expr_list => unreachable!("parse error: should not be "),
        Rule::expr => {
            let pair = pair.into_inner().next().unwrap();
            match pair.as_rule() {
                Rule::alias_expr => build_agg_ast(pair),
                Rule::aggfunc => build_agg_ast(pair),
                Rule::userfunc => build_agg_ast(pair),
                Rule::column_ident => AstNode::Column(pair.as_str().to_string()),
                _ => unreachable!(
                    "parse error: expr expected alias|agg|ident, but got {:#?}: {}",
                    pair.as_rule(),
                    pair.as_str()
                ),
            }
        }
        Rule::userfunc => {
            let mut pairs = pair.into_inner();

            let function = pairs.next().unwrap().as_str().to_owned();
            let column = pairs.next().unwrap().as_str().to_owned();
            let range = match pairs.peek().unwrap().as_rule() {
                Rule::number => Some((
                    pairs.next().unwrap().as_str().parse::<f64>().unwrap(),
                    pairs.next().unwrap().as_str().parse::<f64>().unwrap(),
                )),
                Rule::ident => None,
                _ => unreachable!("parse error: userfunc expected number or ident next"),
            };
            let alias = pairs.next().unwrap().as_str().to_owned();
            AstNode::UserF {
                function,
                column,
                range,
                alias,
            }
        }
        Rule::alias_expr => {
            let mut pair = pair.into_inner();
            match build_agg_ast(pair.next().unwrap()) {
                AstNode::Aggregation {
                    function,
                    inner,
                    range,
                    alias: _,
                } => AstNode::Aggregation {
                    function,
                    inner,
                    range,
                    alias: pair.next().unwrap().as_str().to_string(),
                },
                _ => unreachable!("aliasexpr must have agg inside"),
            }
        }
        Rule::aggfunc => {
            let alias = pair.as_str().to_owned();
            let mut pairs = pair.into_inner();

            let function = pairs.next().unwrap().as_str().to_owned();
            // let inner = Box::new(build_agg_ast(pairs.next().unwrap()));
            let inner = pairs.next().unwrap().as_str().to_owned();
            let range = pairs.peek().map(|_| {
                (
                    pairs.next().unwrap().as_str().parse::<f64>().unwrap(),
                    pairs.next().unwrap().as_str().parse::<f64>().unwrap(),
                )
            });
            AstNode::Aggregation {
                function,
                inner,
                range,
                alias,
            }
        }
        Rule::column_ident => AstNode::Column(pair.as_str().to_owned()),
        Rule::whereclause => {
            let mut pairs = pair.into_inner();
            let predicate = Box::new(build_agg_ast(pairs.next().unwrap()));
            AstNode::Where { predicate }
        }
        Rule::predicate => {
            let mut pairs = pair.into_inner();
            let lhs = Box::new(build_agg_ast(pairs.next().unwrap()));
            let op_str = pairs.next().unwrap().as_str();
            let op = match op_str {
                ">" => BooleanOp::GreaterThan,
                ">=" => BooleanOp::GreaterThanOrEqualTo,
                "<" => BooleanOp::LessThan,
                "<=" => BooleanOp::LessThanOrEqualTo,
                "==" => BooleanOp::Equal,
                "!=" => BooleanOp::NotEqual,
                "&&" => BooleanOp::And,
                "||" => BooleanOp::Or,
                _ => unreachable!("unsupported boolean operator: {}", op_str),
            };
            let rhs = Box::new(build_agg_ast(pairs.next().unwrap()));
            AstNode::Predicate { lhs, op, rhs }
        }
        Rule::groupbyclause => {
            let pairs = pair.into_inner();
            AstNode::GroupBy {
                //attrs: vec![inner.as_str().to_string()],
                attrs: pairs
                    .map(|p| {
                        let pair = p
                            .into_inner()
                            .next()
                            .expect("groupbyattr should have child");
                        match pair.as_rule() {
                            Rule::column_ident => GroupByAttr::Column(pair.as_str().to_string()),
                            Rule::binattr => {
                                let mut binattr = pair.into_inner();
                                let column = binattr.next().unwrap().as_str().to_string();
                                let unit = binattr.next().unwrap().as_str();
                                GroupByAttr::Bin {
                                    column,
                                    size: unit_to_ms(unit) as u64,
                                    alias: unit.to_string(),
                                }
                            }
                            _ => unreachable!(
                                "groupbyattr must be either column_ident or binattr, got {:#?}",
                                pair.as_rule()
                            ),
                        }
                    })
                    .collect(),
            }
        }
        Rule::groupbywithkeys => {
            unimplemented!("group by with keys not implemented yet");
        }
        Rule::windowclause => {
            let mut pairs = pair.into_inner();
            let mut inner = pairs.next().unwrap().into_inner();
            AstNode::DurationMs(parse_duration_to_ms(&mut inner))
        }
        Rule::limitclause => {
            let pairs = pair.into_inner();
            AstNode::Limit {
                rows: pairs.as_str().parse::<u64>().unwrap(),
            }
        }
        Rule::number => AstNode::Value(pair.as_str().parse::<f64>().unwrap()),
        _ => unreachable!(
            "parse error: unexpected rule, got {:#?}: {}",
            pair.as_rule(),
            pair.as_str()
        ),
    }
}

#[cfg(test)]
mod tests {
    use crate::aggregation::AstNode::*;
    use crate::aggregation::*;
    use crate::pest::Parser;

    fn parse_and_build_aggregation(query_string: &str, expected: Vec<AstNode>) {
        let mut pairs = PQLParser::parse(Rule::aggregation_stmt_list, query_string)
            .unwrap_or_else(|e| panic!("{}", e));
        let mut aggregations = vec![];
        for pair in pairs.next().unwrap().into_inner() {
            aggregations.push(build_agg_ast(pair));
        }
        if expected.len() > 0 {
            assert_eq!(aggregations, expected);
        } else {
            println!("output: {:#?}", aggregations);
        }
    }

    #[test]
    fn parse_intersection() {
        let query_string = "SELECT count(plate) FROM (t1 INTERSECT t2 t3 ON plate);";
        parse_and_build_aggregation(query_string, vec![]);
    }

    #[test]
    fn parse_union() {
        let query_string = "SELECT count(plate) FROM (t1 UNION t2 t3 ON plate);";
        parse_and_build_aggregation(query_string, vec![]);
    }

    #[test]
    fn parse_join() {
        let query_string = "SELECT count(plate) FROM (t1 EQUIJOIN t2 t3 ON plate);";
        parse_and_build_aggregation(query_string, vec![]);
    }

    #[test]
    fn parse_nested_combine() {
        let query_string = "SELECT count(plate) FROM ((t1 INTERSECT t2 t3 ON plate) UNION (t4 INTERSECT t4 ON plate) ON plate);";
        parse_and_build_aggregation(query_string, vec![]);
    }

    #[test]
    fn parse_nested_join() {
        let query_string =
            "SELECT mean(speed) FROM (SELECT user_f(speed,l=30,u=60) as speed FROM (t1 EQUIJOIN t2 ON plate));";
        parse_and_build_aggregation(query_string, vec![]);
    }

    #[test]
    fn parse_where() {
        let query_string =
            "SELECT count(people) FROM t1 WHERE ((t1_day > t2_day) && (x > 10)) || (y < 3);";
        parse_and_build_aggregation(query_string, vec![]);
    }

    // The way the grammar is structured requires predicates to explicitly specify precedence using
    // parens. The above test parses correctly, this one does not, though unfortunately the parser
    // error is a bit indirecta
    #[test]
    #[should_panic(expected = "expected qualifier")]
    fn missing_predicate_precedence() {
        let query_string =
            "SELECT count(people) FROM t1 WHERE (t1_day > t2_day) && (x > 10) || (y < 3);";
        parse_and_build_aggregation(query_string, vec![]);
    }

    //#[test]
    //fn parse_two_aggregations() {
    //    let query_string = "SELECT count(people) FROM t1 AND SELECT cout(people) FROM t2";
    //    parse_and_build_aggregation(query_string, vec![]);
    //}

    #[test]
    fn parse_expr() {
        let query_string = "SELECT (t1.people + t2.people) as total FROM t1, t2;";
        parse_and_build_aggregation(query_string, vec![]);
    }

    #[test]
    fn parse_two_exprs() {
        let query_string =
            "SELECT state,count(state) FROM (SELECT user_get_state(plate) as state FROM t1) GROUP BY state;";
        parse_and_build_aggregation(query_string, vec![]);
    }

    #[test]
    fn parse_limit() {
        let query_string = "SELECT sum(plate) FROM cars LIMIT 100;";
        parse_and_build_aggregation(
            query_string,
            vec![Select {
                exprs: vec![Aggregation {
                    function: String::from("sum"),
                    inner: String::from("plate"),
                    range: None,
                    alias: String::from("sum(plate)"),
                }],
                from: Box::new(TableNames(vec![String::from("cars")])),
                qualifiers: vec![Limit { rows: 100 }],
            }],
        );
    }

    #[test]
    fn aggregation() {
        let query_string = "SELECT sum(people,l=0,u=10) FROM (t1 INTERSECT t2 ON plate);";
        parse_and_build_aggregation(
            query_string,
            vec![AstNode::Select {
                exprs: vec![AstNode::Aggregation {
                    function: String::from("sum"),
                    inner: String::from("people"),
                    range: Some((0.0, 10.0)),
                    alias: String::from("sum(people,l=0,u=10)"),
                }],
                from: Box::new(AstNode::Intersect {
                    tables: vec![
                        AstNode::TableNames(vec![String::from("t2")]),
                        AstNode::TableNames(vec![String::from("t1")]),
                    ],
                    on: Box::new(AstNode::Column(String::from("plate"))),
                }),
                qualifiers: vec![],
            }],
        );
    }
}
