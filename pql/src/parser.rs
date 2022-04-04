use pest::error::Error;
use pest::Parser;

use std::collections::HashMap;

use crate::aggregation::{build_agg_ast, AstNode};
use crate::process::{parse_process_stmt, Column, FixedColumn, ProcessStatement};
use crate::split::{parse_split_stmt, SplitStatement};

#[derive(Parser)]
#[grammar = "pql.pest"]
pub struct PQLParser;

#[derive(Debug)]
pub struct PQLQuery {
    pub split_stmts: HashMap<String, SplitStatement>,
    pub process_stmts: HashMap<String, ProcessStatement>,
    pub select_stmts: Vec<AstNode>,
}

// TODO use this for parse_duration as well
pub fn unit_to_ms(unit: &str) -> f64 {
    match unit {
        "usec" => 1.0 / 1_000.0,
        "ms" => 1.0,
        "sec" => 1_000.0,
        "min" => 1_000.0 * 60.0,
        "hr" => 1_000.0 * 60.0 * 60.0,
        "day" => 1_000.0 * 60.0 * 60.0 * 24.0,
        "week" => 1_000.0 * 60.0 * 60.0 * 24.0 * 7.0,
        "month" => 1_000.0 * 60.0 * 60.0 * 24.0 * 7.0 * 30.0,
        _ => unimplemented!("unknown duration unit"),
    }
}

pub fn parse_duration_to_ms(duration: &mut pest::iterators::Pairs<Rule>) -> u64 {
    let value = duration
        .next()
        .unwrap()
        .as_str()
        .to_owned()
        .parse::<f64>()
        .unwrap();
    let ms = match duration.next().unwrap().as_str() {
        "usec" => value / 1_000.0,
        "ms" => value,
        "sec" => value * 1_000.0,
        "min" => value * 1_000.0 * 60.0,
        "hr" => value * 1_000.0 * 60.0 * 60.0,
        "day" => value * 1_000.0 * 60.0 * 60.0 * 24.0,
        "week" => value * 1_000.0 * 60.0 * 60.0 * 24.0 * 7.0,
        "month" => value * 1_000.0 * 60.0 * 60.0 * 24.0 * 7.0 * 30.0,
        _ => unimplemented!("unknown duration unit"),
    } as u64;
    assert!(
        duration.next().is_none(),
        "parse error: duration expected 2 pairs (value and unit), but got more"
    );
    ms
}

fn replace_tables(
    split_stmts: &HashMap<String, SplitStatement>,
    process_stmts: &HashMap<String, ProcessStatement>,
    node: AstNode,
) -> AstNode {
    match node {
        AstNode::TableNames(ts) => AstNode::Tables(
            ts.iter()
                .map(|t| {
                    let mut ps = process_stmts
                        .get(t)
                        .unwrap_or_else(|| panic!("unknown table used in aggregation: {}", t))
                        .clone();
                    let ss = split_stmts
                        .get(&ps.input_name)
                        .unwrap_or_else(|| {
                            panic!("unknown chunks used in PROCESS: {}", ps.input_name)
                        })
                        .clone();
                    // manually add fixed columns here
                    // TODO also add region here
                    // TODO maybe this should be in a different place?
                    ps.schema.push(Column::Fixed(FixedColumn {
                        name: String::from("chunk"),
                        range: (ss.start_time, ss.end_time),
                    }));
                    AstNode::Table(ss, ps)
                })
                .collect(),
        ),
        AstNode::Select {
            exprs,
            from,
            qualifiers,
        } => AstNode::Select {
            exprs,
            from: Box::new(replace_tables(split_stmts, process_stmts, *from)),
            qualifiers,
        },
        AstNode::Intersect { tables, on } => AstNode::Intersect {
            tables: tables
                .iter()
                .map(|t| replace_tables(split_stmts, process_stmts, t.to_owned()))
                .collect(),
            on,
        },
        AstNode::Union { tables, on } => AstNode::Union {
            tables: tables
                .iter()
                .map(|t| replace_tables(split_stmts, process_stmts, t.to_owned()))
                .collect(),
            on,
        },
        AstNode::Equijoin { tables, on } => AstNode::Equijoin {
            tables: tables
                .iter()
                .map(|t| replace_tables(split_stmts, process_stmts, t.to_owned()))
                .collect(),
            on,
        },
        _ => node,
    }
}

pub fn build(query_str: &str) -> Result<PQLQuery, Error<Rule>> {
    let mut pairs = PQLParser::parse(Rule::query, query_str).unwrap_or_else(|e| panic!("{}", e));

    // Splits
    let mut split_stmts: HashMap<String, SplitStatement> = HashMap::new();
    let mut process_stmts: HashMap<String, ProcessStatement> = HashMap::new();
    for pair in pairs.next().unwrap().into_inner() {
        match pair.as_rule() {
            Rule::split_stmt => {
                let stmt = parse_split_stmt(pair);
                split_stmts.insert(stmt.output_name.clone(), stmt);
            }
            Rule::process_stmt => {
                let stmt = parse_process_stmt(pair);
                process_stmts.insert(stmt.output_table_name.clone(), stmt);
            }
            _ => {
                unreachable!(format!(
                    "query must begin with SPLITs or PROCESSs, got: {:?} = {:#?}",
                    pair.as_rule(),
                    pair
                ));
            }
        }
    }

    // Aggregations
    let mut select_stmts = vec![];
    for pair in pairs.next().unwrap().into_inner() {
        let stmt = build_agg_ast(pair);
        select_stmts.push(replace_tables(&split_stmts, &process_stmts, stmt));
    }

    Ok(PQLQuery {
        split_stmts,
        process_stmts,
        select_stmts,
    })
}

#[cfg(test)]
mod tests {

    #[test]
    fn simple_query() {
        let query_string = "SPLIT cam1
            BEGIN 0
            END 0
            BY TIME 5sec
            INTO chunks1;

            PROCESS chunks1 USING yolov3 TIMEOUT 1sec
            PRODUCING 10 ROWS
            WITH SCHEMA (plate:STRING=null, speed:NUMBER=0.75)
            INTO table1;

        SELECT count(plate) FROM table1;";

        crate::parser::build(query_string).unwrap();
    }

    #[test]
    fn two_tables() {
        let query_string = "SPLIT cam1
            BEGIN 0
            END 0
            BY TIME 5sec
            INTO chunks1;

            PROCESS chunks1 USING yolov3 TIMEOUT 1sec
            PRODUCING 10 ROWS
            WITH SCHEMA (plate:STRING=null, speed:NUMBER=0.75)
            INTO table1;

        SPLIT cam2
            BEGIN 0
            END 0
            BY TIME 5sec
            INTO chunks2;

            PROCESS chunks2 USING yolov3 TIMEOUT 1sec
            PRODUCING 10 ROWS
            WITH SCHEMA (plate:STRING=null, speed:NUMBER=0.75)
            INTO table2;

        SELECT count(plate) FROM (table1 UNION table2 ON plate);";

        crate::parser::build(query_string).unwrap();
    }

    //#[test]
    //#[should_panic(expected = "unknown table used in aggregation: t2")]
    //fn simple_query_missing_table() {
    //    let query_string = "CREATE t1 FROM cam1
    //        START AT 0
    //        END AT 10
    //        PARTITION BY TIME(10.5secs, 0secs)
    //        PROCESS USING yolov3
    //        FOR AT MOST 1sec
    //        PRODUCE [(plate,STRING,null),(speed,NUMBER,0.743)]
    //        LIMIT 1

    //        THEN
    //
    //        SELECT count(plate) FROM t2";

    //    crate::parser::parse(query_string).unwrap();
    //}
}
