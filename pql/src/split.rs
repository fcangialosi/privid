use crate::parser::*;

#[derive(Debug, PartialEq, Clone)]
pub struct SplitStatement {
    // Unique reference to the camera, used to fetch the proper video source
    pub camera_name: String,

    // Time to start processing data from camera
    pub start_time: u64,
    // Time to stop processing video from camera.
    // If this date is in the future, the query will be treated as an online query
    // and values will be released at the end of each window
    pub end_time: u64,

    pub chunk_length_ms: u64,
    // TODO stride
    // pub chunk_stride_ms: u64,

    // TODO regions

    // Name of chunks that will be created from this video
    pub output_name: String,
}

pub fn parse_split_stmt(pair: pest::iterators::Pair<Rule>) -> SplitStatement {
    match pair.as_rule() {
        Rule::split_stmt => {
            let mut pair = pair.into_inner();
            let camera_name = pair.next().unwrap().as_str().to_owned();
            let start_time = pair
                .next()
                .unwrap()
                .as_str()
                .to_owned()
                .parse::<u64>()
                .unwrap();
            let end_time = pair
                .next()
                .unwrap()
                .as_str()
                .to_owned()
                .parse::<u64>()
                .unwrap();
            let chunk_length_ms = parse_duration_to_ms(&mut pair.next().unwrap().into_inner());
            // TODO add optional stride later
            // let next = pair.next().unwrap();
            // let chunk_stride_ms = if next.as_rule() == Rule::stride {
            //     parse_duration_to_ms(&mut next.into_inner())
            // } else {
            //     0
            // };
            let output_name = pair.next().unwrap().as_str().to_owned();

            SplitStatement {
                camera_name,
                start_time,
                end_time,
                chunk_length_ms,
                output_name,
            }
        }
        _ => unreachable!("parse error: expected split, got {:#?}", pair.as_rule()),
    }
}

#[cfg(test)]
mod tests {
    use crate::pest::Parser;
    use crate::split::*;

    fn parse_and_build_split_stmt(split_str: &str, expected: SplitStatement) {
        // string -> pest obj
        let pair = PQLParser::parse(Rule::split_stmt, split_str)
            .unwrap_or_else(|e| panic!("{}", e))
            .next()
            .unwrap();
        // pest obj -> SplitStatement object
        let parsed_stmt = parse_split_stmt(pair);
        assert_eq!(parsed_stmt, expected);
    }

    #[test]
    fn parse_simple_split() {
        let split_str = "SPLIT cam1
            BEGIN 0 
            END 10
            BY TIME 10.5sec
            INTO chunks1;";
        parse_and_build_split_stmt(
            split_str,
            SplitStatement {
                camera_name: String::from("cam1"),
                start_time: 0,
                end_time: 10,
                chunk_length_ms: 10500,
                // chunk_stride_ms: 1000,
                output_name: String::from("chunks1"),
            },
        );
    }
}
