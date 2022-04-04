# PQL

PQL is implemented in Rust, and relies heavily on the [Pest](https://pest.rs/) library to handle parsing. 

The language grammar (a parsing expression grammar, or PEG) is specified in `src/pql.pest`.

The sensitivity calculation is implemented in `src/sensitivity.rs`. This is a direct implementation of the rules specified in Appendix B and Figure 9 of our paper.

## Build

To build the parser and calculator:
1. Download rust and cargo (use [rustup.rs](https://rustup.rs)
2. `cargo build`

