//#[derive(PartialEq, Debug, Clone)]
//pub enum AggregationFunction {
//    Count {
//        column: Box<Column>,
//    },
//    Sum {
//        column: Box<NumberColumn>,
//        lower: f64,
//        upper: f64,
//    },
//    Mean {
//        column: Box<NumberColumn>,
//        lower: f64,
//        upper: f64,
//        window: f64,
//    },
//    User {
//        name: String,
//        column: Box<NumberColumn>,
//        lower: f64,
//        upper: f64,
//    },
//}
//
//
use crate::aggregation::{AstNode, GroupByAttr};
use crate::policy::{PolicyMap, PrivacyPolicy};
use crate::process::{Column, ProcessStatement};
use crate::split::SplitStatement;

use std::collections::HashMap;

type RowSensitivity = u64;
// TODO eventually need to change this to range constarint and add a size constraint
#[derive(Debug, Clone, PartialEq)]
enum ColumnInfo {
    Release(f64),
    Range((f64, f64)),
    Nan,
    Unbound,
}
type ColumnMap = HashMap<String, ColumnInfo>;

// TODO add string explanation of how the sensitivity was computed for easy debugging
#[derive(Debug, PartialEq)]
pub struct TableSensitivity {
    rows: RowSensitivity,
    columns: ColumnMap,
    size_constraint: Option<u64>,
}
impl TableSensitivity {
    fn empty() -> Self {
        TableSensitivity {
            rows: 0,
            columns: ColumnMap::new(),
            size_constraint: Some(0),
        }
    }
}

#[allow(dead_code, unused_variables)]
pub fn table_sensitivity(
    split_stmt: &SplitStatement,
    process_stmt: &ProcessStatement,
    policies: &PolicyMap,
) -> u64 {
    let policy = policies.get(&split_stmt.camera_name).expect(
        format!(
            "policymap does not contain policy for camera '{}'",
            split_stmt.camera_name
        )
        .as_str(),
    );
    match policy {
        PrivacyPolicy::Static {
            k_segments,
            epsilon,
            rho_ms,
        } => {
            let chunks =
                1 + (rho_ms.clone() as f64 / split_stmt.chunk_length_ms as f64).ceil() as u64;
            let rows = process_stmt.maxrow * k_segments * chunks;
            rows
        }
        PrivacyPolicy::Mask {} => {
            unimplemented!("mask privacy policies not implemented yet");
        }
    }
}

// TODO also output the number of rows the table will have to provide an accuracy bound
// when composing tables need to propogate this value
#[allow(dead_code, unused_variables)]
pub fn sensitivity_composition(node: &AstNode, policies: &PolicyMap) -> TableSensitivity {
    match node {
        AstNode::Table(ss, ps) => TableSensitivity {
            rows: table_sensitivity(ss, ps, policies),
            columns: ps
                .schema
                .iter()
                .map(|c| match c {
                    Column::String(sc) => (
                        ps.output_table_name.clone() + "." + sc.name.as_str(),
                        ColumnInfo::Nan,
                    ),
                    Column::Number(nc) => (
                        ps.output_table_name.clone() + "." + nc.name.as_str(),
                        ColumnInfo::Unbound,
                    ),
                    Column::Fixed(fc) => {
                        let (s, e) = fc.range;
                        (
                            // TODO ?
                            //ps.output_table_name.clone() + "." + fc.name.as_str(),
                            fc.name.as_str().to_string(),
                            ColumnInfo::Range((s as f64 * 1000.0, e as f64 * 1000.0)),
                        )
                    }
                })
                .collect::<ColumnMap>(),
            size_constraint: {
                let video_length_ms = ((ss.end_time - ss.start_time) * 1000) as f64;
                let num_chunks = video_length_ms / ss.chunk_length_ms as f64;
                Some(num_chunks.ceil() as u64)
            },
        },
        AstNode::Tables(tables) => {
            // TODO temp hack
            sensitivity_composition(tables.get(0).unwrap(), policies)
        }
        AstNode::Intersect { tables, on } => tables
            .iter()
            .map(|t| sensitivity_composition(t, policies))
            .fold(TableSensitivity::empty(), |mut acc, s| {
                acc.rows += s.rows;
                acc.columns.extend(s.columns);
                acc.size_constraint = std::cmp::max(acc.size_constraint, s.size_constraint);
                acc
            }),
        AstNode::Union { tables, on } => {
            tables
                .iter()
                .map(|t| sensitivity_composition(t, policies))
                .fold(TableSensitivity::empty(), |mut acc, s| {
                    acc.rows += s.rows;
                    acc.columns.extend(s.columns);
                    // TODO assuming tables cover the same time range for now, in the future need
                    // to compute the actual size of the table based on the set of time ranges
                    // covered across all tables
                    acc.size_constraint = std::cmp::max(acc.size_constraint, s.size_constraint);
                    // acc.size_constraint = match (acc.size_constraint, s.size_constraint) {
                    //     (None, _) => None,
                    //     (_, None) => None,
                    //     (Some(a), Some(sc)) => Some(a + sc),
                    // };
                    // TODO chunk column
                    acc
                })
        }
        AstNode::Equijoin { tables, on } => tables
            .iter()
            .map(|t| sensitivity_composition(t, policies))
            .fold(TableSensitivity::empty(), |mut acc, s| {
                acc.rows += s.rows;
                acc.columns.extend(s.columns);
                // TODO chunk column
                acc
            }),
        AstNode::Select {
            exprs,
            from,
            qualifiers,
        } => {
            let mut base = sensitivity_composition(from, policies);

            let mut ret = TableSensitivity {
                rows: base.rows,
                columns: HashMap::new(),
                size_constraint: base.size_constraint,
            };

            for q in qualifiers {
                match q {
                    AstNode::GroupBy { attrs } => {
                        let mut table_to_col: HashMap<String, Vec<String>> = HashMap::new();
                        for (name, info) in &base.columns {
                            if name.contains(".") {
                                let mut sp = name.split(".");
                                table_to_col
                                    .entry(sp.next().unwrap().to_string())
                                    .or_default()
                                    .push(sp.next().unwrap().to_string());
                            }
                            // else {
                            //     table_to_col
                            //         .entry(String::from("."))
                            //         .or_default()
                            //         .push(name.clone());
                            // }
                        }
                        for attr in attrs {
                            match attr {
                                GroupByAttr::Column(colname) => {
                                    if table_to_col.iter().all(|(_, cs)| cs.contains(colname)) {
                                        base.columns.insert(colname.to_owned(), ColumnInfo::Nan);
                                    }
                                }
                                GroupByAttr::Bin {
                                    column,
                                    size,
                                    alias,
                                } => {
                                    let prev = match base.columns.get(column) {
                                        Some(ColumnInfo::Range((l, u))) => (u - l),
                                        _ => unreachable!("chunk should be range..."),
                                    };
                                    base.columns.insert(
                                        alias.to_owned(),
                                        ColumnInfo::Range((0.0, prev / *size as f64)),
                                    );
                                    // TODO TEMP HACK
                                    base.size_constraint = Some((prev / *size as f64) as u64);
                                    ret.size_constraint = Some((prev / *size as f64) as u64);
                                }
                            }
                        }
                    }
                    AstNode::Limit { rows } => {
                        // TODO TEMP HACK
                        base.size_constraint = Some(*rows);
                    }
                    _ => {
                        unimplemented!("this qualifier not implemented yet");
                    }
                }
            }

            for expr in exprs {
                match expr {
                    // Pull from the base table
                    AstNode::Column(c) => {
                        ret.columns.insert(
                            c.clone(),
                            base.columns
                                .get(c)
                                .unwrap_or_else(|| {
                                    panic!("{} does not exist in {:#?}, base: {:#?}", c, from, base)
                                })
                                .to_owned(),
                        );
                    }
                    // If it's a userf, we use their range provided if possible
                    AstNode::UserF {
                        function,
                        column,
                        range,
                        alias,
                    } => {
                        ret.columns.insert(
                            alias.clone(),
                            range
                                .to_owned()
                                .map_or(ColumnInfo::Unbound, |r| ColumnInfo::Range(r)),
                        );
                    }
                    AstNode::Aggregation {
                        function,
                        inner,
                        range: explicit_range,
                        alias,
                    } => match function.to_lowercase().as_str() {
                        "sum" => {
                            let res = explicit_range.map_or(ColumnInfo::Unbound, |(l, u)| {
                                ColumnInfo::Release((u - l) * base.rows as f64)
                            });
                            ret.columns.insert(alias.clone(), res);
                        }
                        "mean" => {
                            let range = explicit_range.map_or_else(
                                || {
                                    base.columns
                                        .get(inner)
                                        .unwrap_or(&ColumnInfo::Unbound)
                                        .to_owned()
                                },
                                |r| ColumnInfo::Range(r),
                            );
                            let res = match range {
                                ColumnInfo::Range((l, u)) => {
                                    base.size_constraint.map_or(range, |sc| {
                                        println!(
                                            "alias: {}, range: ({},{}), rows: {}, sc: {}",
                                            alias, l, u, base.rows, sc,
                                        );
                                        ColumnInfo::Release((u - l) * base.rows as f64 / sc as f64)
                                    })
                                }
                                ColumnInfo::Release(r) => {
                                    base.size_constraint.map_or(range, |sc| {
                                        println!(
                                            "alias: {}, release: {}, rows: {}, sc: {}",
                                            alias, r, base.rows, sc,
                                        );
                                        ColumnInfo::Release(r / sc as f64)
                                    })
                                }
                                _ => ColumnInfo::Unbound,
                            };
                            ret.columns.insert(alias.clone(), res);
                        }
                        "count" => {
                            //ret.columns
                            //    .insert(alias.clone(), ColumnInfo::Range((0.0, 1.0)));
                            ret.columns
                                .insert(alias.clone(), ColumnInfo::Range((0.0, base.rows as f64)));
                            ret.rows = 1;
                        }
                        "var" => {
                            unimplemented!("TODO: var()");
                        }
                        "stddev" => {
                            unimplemented!("TODO: stddev()");
                        }
                        _ => unimplemented!("unsupported aggregation function {}", function),
                    },
                    _ => unreachable!("expr can only be col, userf, aggregation, or alias"),
                }
            }

            // TODO Ignoring for now, because it should be the same for each key
            // for qualifier in qualifiers {
            //     match qualifier {
            //         AstNode::Where { predicate } => { /* doesn't impact sensitivity */ }
            //         AstNode::GroupBy { attr } => {
            //             // if this is inner groupby, probably not specifying keys (eg plate)
            //             // if this is outer, could be either, e.g. (count for each of male/female)
            //         }
            //         AstNode::DurationMs(d) => match ret.columns.get("chunk") {
            //             Some(ColumnInfo::Range((a, b))) => {
            //                 // TODO warn rounding
            //                 for i in (a.round() as u64..b.round() as u64).step_by(
            //                     (d / 1000)
            //                         .try_into()
            //                         .expect("step size doesnt fit in usize!"),
            //                 ) {}
            //             }
            //             _ => unreachable!(
            //                 "cannot WINDOW a joined table which has removed temporal element"
            //             ),
            //         },
            //         _ => {
            //             unreachable!("qualifier must be one of where, groupby, or window");
            //         }
            //     };
            // }
            println!("ret: {:#?}", ret);
            ret
        }
        //AstNode::AliasExpr {
        //    lhs,
        //    op,
        //    rhs,
        //    alias,
        //} => {
        //    let mut ranges = HashMap::new();
        //    let (lhs_min, lhs_max) = sensitivity_composition(lhs, policy)
        //        .ranges
        //        .get("self")
        //        .unwrap()
        //        .unwrap();
        //    let (rhs_min, rhs_max) = sensitivity_composition(rhs, policy)
        //        .ranges
        //        .get("self")
        //        .unwrap()
        //        .unwrap();
        //    let new_range = match op {
        //        ArithmeticOp::Plus => (lhs_min + rhs_min, lhs_max + rhs_max),
        //        ArithmeticOp::Minus => (lhs_min - rhs_min, lhs_max - rhs_max),
        //        _ => unimplemented!(
        //            "only + and - are currently supported for expressions among aggregations"
        //        ),
        //    };
        //    ranges.insert(alias.to_owned(), Some(new_range));
        //    TableSensitivity { rows: 1, ranges }
        //}
        _ => unimplemented!("not done yet: {:#?}", node),
    }
}

//pub fn calculate_sensitivity(node: qstNode, policy: PrivacyPolicy) -> Sensitivity {
//    match node {
//        AstNode::T(TableNode::Base(t)) => {
//            let rho_tilde = (policy.rho_ms as f64 * policy.multiplier).ceil() as u32;
//            let s = (((rho_tilde + std::cmp::max(t.chunk_length_ms, t.chunk_stride_ms)) as f64)
//                / ((t.chunk_length_ms + t.chunk_stride_ms) as f64))
//                .ceil() as u32;
//
//            Sensitivity {
//                rows: (((1 * (s + 1)) + ((policy.k_segments - 1) * 2)) * t.maxrow),
//                ranges: t
//                    .output
//                    .iter()
//                    .map(|c| match c {
//                        Column::String(sc) => (sc.name, None),
//                        Column::Number(nc) => (nc.name, None),
//                    })
//                    .collect::<ColumnMap>(),
//            }
//        }
//        AstNode::T(TableNode::Union(UnionStatement {
//            exprs,
//            tables,
//            on_column,
//        })) => {
//            let s = tables
//                .iter()
//                .map(|t| calculate_sensitivity(AstNode::T(*t), policy))
//                .fold(
//                    Sensitivity {
//                        rows: 0,
//                        ranges: HashMap::new(),
//                    },
//                    |acc, ts| {
//                        acc.rows += ts.rows;
//                        for (k, v) in ts.ranges {
//                            acc.ranges.insert(k, v);
//                        }
//                        acc
//                    },
//                );
//            for expr in exprs {
//                let expr_s = calculate_sensitivity(AstNode::E(expr), policy);
//
//            }
//            s
//        }
//        AstNode::T(TableNode::Intersection(IntersectStatement { tables, on_column })) => tables
//            .iter()
//            .map(|t| calculate_sensitivity(AstNode::T(*t), policy))
//            .min()
//            .unwrap(),
//        AstNode::T(TableNode::EquiJoin(EquiJoinStatement {
//            exprs,
//            tables,
//            on_column,
//        })) => tables
//            .iter()
//            .map(|t| calculate_sensitivity(AstNode::T(*t), policy))
//            .min()
//            .unwrap(),
//        AstNode::A(AggregationFunction::Count { column }) => {
//            let parent = match *column {
//                Column::String(col) => AstNode::T(TableNode::Base(col.parent)),
//                Column::Number(col) => AstNode::T(TableNode::Base(col.parent)),
//            };
//            let delta_t = calculate_sensitivity(parent, policy);
//            delta_t * 1
//        }
//        AstNode::A(AggregationFunction::Sum {
//            column,
//            lower,
//            upper,
//        }) => {
//            let s = calculate_sensitivity(AstNode::T(TableNode::Base(column.parent)), policy);
//            s.ranges.delta_t * (upper - lower).abs()
//        }
//        AstNode::S(AggregationStatement { exprs, from }) => {
//            let s = calculate_sensitivity(AstNode::T(from), policy);
//            let es = exprs
//                .iter()
//                .map(|e| calculate_sensitivity(AstNode::E(*e), policy));
//            0
//        }
//        AstNode::E(Expression::Column(col)) => 1,
//        AstNode::E(Expression::Aggregation { func, alias }) => {
//            let delta_f = calculate_sensitivity(AstNode::A(func), policy);
//            0
//        }
//        AstNode::E(Expression::Dyadic { lhs, rhs, op }) => {
//            let delta_l = calculate_sensitivity(AstNode::E(*lhs), policy);
//            let delta_r = calculate_sensitivity(AstNode::E(*rhs), policy);
//
//            let x = 1;
//            0
//        }
//        _ => unimplemented!("yikes"),
//    }
//}

//impl From<Box<CreationStatement>> for AstNode {
//    fn from(c: Box<CreationStatement>) -> Self {
//        AstNode::T(TableNode::Base(c))
//    }
//}

//pub struct AggregationStatement {
//    exprs: Vec<AstNode>,
//    from: AstNode,
//    qualifiers: Vec<AstNode>,
//}
//
//#[derive(PartialEq, Debug, Clone)]
//pub enum Expression {
//    Column(Box<Column>),
//    Aggregation {
//        func: AggregationFunction,
//        alias: String,
//    },
//    Dyadic {
//        lhs: Box<Expression>,
//        rhs: Box<Expression>,
//        op: ArithmeticOp,
//    },
//}
//
//#[derive(PartialEq, Debug, Clone)]
//pub enum TableNode {
//    Base(CreationStatement),
//    Union(UnionStatement),
//    EquiJoin(EquiJoinStatement),
//    Intersection(IntersectStatement),
//}
//
//#[derive(PartialEq, Debug, Clone)]
//pub struct UnionStatement {
//    exprs: Vec<Expression>,
//    tables: Vec<TableNode>, // something to enforce
//    on_column: String,
//}
//
//#[derive(PartialEq, Debug, Clone)]
//pub struct EquiJoinStatement {
//    exprs: Vec<Expression>,
//    tables: Vec<TableNode>, // something to enforce these are only one of the expected Table types
//    on_column: String,
//}
//
//#[derive(PartialEq, Debug, Clone)]
//pub struct IntersectStatement {
//    tables: Vec<TableNode>, // something to enforce
//    on_column: String,
//}

#[cfg(test)]
mod tests {
    use crate::policy::{PolicyMap, PrivacyPolicy};
    use crate::sensitivity::*;

    // 70
    fn table_one() -> &'static str {
        "SPLIT cam1
            BEGIN 0
            END 0
            BY TIME 5sec
            INTO chunks1;

            PROCESS chunks1 USING yolov3 TIMEOUT 1sec
            PRODUCING 10 ROWS
            WITH SCHEMA (plate:STRING=null, speed:NUMBER=0)
            INTO table1;
        "
    }

    // 62
    fn table_two() -> &'static str {
        "SPLIT cam2
            BEGIN 0
            END 0
            BY TIME 1sec
            INTO chunks2;

        PROCESS chunks2 USING yolov3 TIMEOUT 1sec
            PRODUCING 2 ROWS
            WITH SCHEMA (plate:STRING=null, speed:NUMBER=0)
            INTO table2;
        "
    }

    fn policies() -> PolicyMap {
        vec![
            (
                String::from("cam1"),
                PrivacyPolicy::Static {
                    k_segments: 1,
                    rho_ms: 30_000,
                    epsilon: 1.0,
                },
            ),
            (
                String::from("cam2"),
                PrivacyPolicy::Static {
                    k_segments: 1,
                    rho_ms: 30_000,
                    epsilon: 1.0,
                },
            ),
            (
                String::from("auburn"),
                PrivacyPolicy::Static {
                    k_segments: 1,
                    rho_ms: 49_000,
                    epsilon: 1.0,
                },
            ),
        ]
        .into_iter()
        .collect()
    }

    fn check_result(table_str: &str, policies: PolicyMap, select_str: &str, expected: f64) {
        let query_string = format!("{} {}", table_str, select_str);
        let query = crate::parser::build(query_string.as_str()).unwrap();
        let res = sensitivity_composition(query.select_stmts.first().unwrap(), &policies);
        // This computes the final step of multiplying the max impact on a column by the max rows
        let (_, val) = res.columns.iter().next().expect("no columns bound");
        let release_sensitivity = match val {
            // ColumnInfo::Range((l, u)) => (u - l) * res.rows as f64,
            ColumnInfo::Release(x) => x.to_owned(),
            _ => {
                unimplemented!("got res: {:#?}", res);
            }
        };
        if expected == 0.0 {
            println!("output: {:#?}\nsensitivity: {}", res, release_sensitivity);
        } else {
            assert_eq!(release_sensitivity, expected);
        }
    }

    #[test]
    fn simple_count() {
        check_result(
            table_one(),
            policies(),
            "SELECT count(plate) FROM table1;",
            70.0,
        );
    }

    #[test]
    fn simple_sum() {
        check_result(
            table_one(),
            policies(),
            "SELECT sum(speed,l=0,u=10) FROM table1;",
            700.0,
        );
    }

    #[test]
    fn simple_union() {
        check_result(
            (table_one().to_owned() + table_two()).as_str(),
            policies(),
            "SELECT count(plate) FROM (table1 UNION table2 ON plate);",
            132.0,
        );
    }

    // 64
    fn table_fig5() -> &'static str {
        "SPLIT auburn
            BEGIN 0
            END 43200
            BY TIME 30sec
            INTO auburn_chunks;

        PROCESS auburn_chunks USING auburn.py TIMEOUT 1sec
            PRODUCING 1 ROWS
            WITH SCHEMA (ppl:NUMBER=0)
            INTO auburnPpl;
        "
    }

    #[test]
    fn simple_window() {
        check_result(
            table_fig5(),
            policies(),
            "SELECT sum(ppl,l=0,u=6) FROM auburnPpl WITH WINDOW 2 hrs ;",
            18.0,
        )
    }

    fn table_porto() -> &'static str {
        "
        SPLIT porto10
            BEGIN 0 END 31536000
            BY TIME 15sec
            INTO chunks10;
        SPLIT porto27
            BEGIN 0 END 31536000
            BY TIME 15sec
            INTO chunks27;
        PROCESS chunks10 USING porto.py TIMEOUT 1sec
            PRODUCING 3 ROWS
            WITH SCHEMA (plate:STRING=null)
            INTO table10;
        PROCESS chunks27 USING porto.py TIMEOUT 1sec
            PRODUCING 3 ROWS
            WITH SCHEMA (plate:STRING=null)
            INTO table27;
        "
    }

    fn policies_porto() -> PolicyMap {
        vec![
            (
                String::from("porto10"),
                PrivacyPolicy::Static {
                    k_segments: 1,
                    epsilon: 1.0,
                    rho_ms: 45_000,
                },
            ),
            (
                String::from("porto27"),
                PrivacyPolicy::Static {
                    k_segments: 1,
                    epsilon: 1.0,
                    rho_ms: 195_000,
                },
            ),
        ]
        .into_iter()
        .collect()
    }

    #[test]
    fn porto_union() {
        check_result(
            table_porto(),
            policies_porto(),
            "SELECT mean(avg_shift) FROM 
                (SELECT mean(shift) as avg_shift FROM
                    (SELECT plate,day,USER_shift(chunk,l=0,u=16) as shift FROM 
                    (table10 UNION table27 ON plate) GROUP BY plate,bin(chunk,day))
                GROUP BY plate LIMIT 300);",
            0.0,
        );
    }

    #[test]
    fn porto_intersection() {
        check_result(
            table_porto(),
            policies_porto(),
            "SELECT mean(perday) FROM 
                (SELECT day,count(plate) as perday FROM 
                    (table10 INTERSECT table27 ON plate) GROUP BY plate,bin(chunk,day));",
            0.0,
        );
    }

    #[test]
    fn porto_argmax() {
        check_result(
            table_porto(),
            // TODO implement "table" column and argmax
            // TODO maybe implement privacy policy decrementing?
            policies_porto(),
            "SELECT argmax(table) FROM 
                (SELECT table,mean(perday) FROM 
                    (SELECT table,day,count(plate) as perday FROM 
                        (table10 UNION table27 ON plate) 
                    GROUP BY table,bin(chunk,day)));",
            0.0,
        );
    }

    // TODO implement python interface
    // input: string query, privacy policy hashmap
    // output map of result to sensitivity
    // calling code is responsible for checking and decrementing privacy budget
    // should have a function in python that looks like the one in the paper
}
