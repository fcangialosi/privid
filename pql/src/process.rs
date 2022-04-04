use crate::parser::*;

#[derive(Debug, PartialEq, Clone)]
pub struct ProcessStatement {
    // Name of chunks object to be processed
    pub input_name: String,

    // Unique reference to the model that will be loaded and used per chunk
    pub model_name: String,

    pub chunk_timeout_ms: u64,

    // The maximum number of rows (with format described by `output`) that each chunk may output
    pub maxrow: u64,

    // Description of columns of the rows output by each chunk
    pub schema: Vec<Column>,

    // Name of table produced by processing chunks
    pub output_table_name: String,
}

#[derive(Debug, PartialEq, Clone)]
pub struct StringColumn {
    pub name: String,
    pub default: String,
}

#[derive(Debug, PartialEq, Clone)]
pub struct NumberColumn {
    pub name: String,
    pub default: f64,
}

#[derive(Debug, PartialEq, Clone)]
pub struct FixedColumn {
    pub name: String,
    pub range: (u64, u64),
}

#[derive(Debug, PartialEq, Clone)]
pub enum Column {
    String(StringColumn),
    Number(NumberColumn),
    Fixed(FixedColumn),
}

fn parse_columns(def_list: pest::iterators::Pairs<Rule>) -> Vec<Column> {
    let mut cols = vec![];
    for def in def_list {
        for col in def.into_inner() {
            let col_type = col.as_rule();
            let mut col = col.into_inner();
            cols.push(match col_type {
                Rule::column_def_string => Column::String(StringColumn {
                    name: col.next().unwrap().as_str().to_owned(),
                    default: col.next().unwrap().as_str().to_owned(),
                }),
                Rule::column_def_num => Column::Number(NumberColumn {
                    name: col.next().unwrap().as_str().to_owned(),
                    default: col
                        .next()
                        .unwrap()
                        .as_str()
                        .to_owned()
                        .parse::<f64>()
                        .unwrap(),
                }),
                _ => unreachable!("only string or number columns allowed"),
            });
        }
    }

    cols
}

pub fn parse_process_stmt(pair: pest::iterators::Pair<Rule>) -> ProcessStatement {
    match pair.as_rule() {
        Rule::process_stmt => {
            let mut pair = pair.into_inner();
            let input_name = pair.next().unwrap().as_str().to_owned();
            let model_name = pair.next().unwrap().as_str().to_owned();
            let chunk_timeout_ms = parse_duration_to_ms(&mut pair.next().unwrap().into_inner());
            let maxrow = pair
                .next()
                .unwrap()
                .as_str()
                .to_owned()
                .parse::<u64>()
                .unwrap();
            let schema = parse_columns(pair.next().unwrap().into_inner());
            let output_table_name = pair.next().unwrap().as_str().to_owned();

            ProcessStatement {
                input_name,
                model_name,
                chunk_timeout_ms,
                maxrow,
                schema,
                output_table_name,
            }
        }
        _ => unreachable!(
            "parse error: expected process statement, got {:#?}",
            pair.as_rule()
        ),
    }
}

#[cfg(test)]
mod tests {
    use crate::pest::Parser;
    use crate::process::*;

    fn parse_and_build_process_stmt(process_str: &str, expected: ProcessStatement) {
        // string -> pest obj
        let pair = PQLParser::parse(Rule::process_stmt, process_str)
            .unwrap_or_else(|e| panic!("{}", e))
            .next()
            .unwrap();
        // pest obj -> ProcessStatement object
        let parsed_stmt = parse_process_stmt(pair);
        assert_eq!(parsed_stmt, expected);
    }

    #[test]
    fn parse_simple_process() {
        let process_str = "PROCESS chunks1
            USING yolov3
            TIMEOUT 1sec
            PRODUCING 10 ROWS
            WITH SCHEMA (plate:STRING=null, speed:NUMBER=0.75)
            INTO table1;";
        parse_and_build_process_stmt(
            process_str,
            ProcessStatement {
                input_name: String::from("chunks1"),
                model_name: String::from("yolov3"),
                chunk_timeout_ms: 1000,
                maxrow: 10,
                schema: vec![
                    Column::String(StringColumn {
                        name: String::from("plate"),
                        default: String::from("null"),
                    }),
                    Column::Number(NumberColumn {
                        name: String::from("speed"),
                        default: 0.75,
                    }),
                ],
                output_table_name: String::from("table1"),
            },
        );
    }
}
