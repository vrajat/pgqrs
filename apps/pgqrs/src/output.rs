use clap::ValueEnum;
use serde::Serialize;
use std::fmt;
use tabled::{Table, Tabled};
use colored::*;

#[derive(Debug, Clone, ValueEnum)]
pub enum OutputFormat {
    /// Display results in a human-readable table
    Table,
    /// Display results as JSON
    Json,
}

impl fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OutputFormat::Table => write!(f, "table"),
            OutputFormat::Json => write!(f, "json"),
        }
    }
}

pub struct OutputManager {
    format: OutputFormat,
    quiet: bool,
}

impl OutputManager {
    pub fn new(format: OutputFormat, quiet: bool) -> Self {
        Self { format, quiet }
    }

    pub fn print_success<T>(&self, data: &T) -> anyhow::Result<()>
    where
        T: Serialize + Tabled,
    {
        if self.quiet {
            return Ok(());
        }

        match self.format {
            OutputFormat::Json => {
                let json = serde_json::to_string_pretty(data)?;
                println!("{}", json);
            }
            OutputFormat::Table => {
                let table = Table::new([data]);
                println!("{}", table);
            }
        }
        Ok(())
    }

    pub fn print_list<T>(&self, data: &[T]) -> anyhow::Result<()>
    where
        T: Serialize + Tabled,
    {
        if self.quiet {
            return Ok(());
        }

        match self.format {
            OutputFormat::Json => {
                let json = serde_json::to_string_pretty(data)?;
                println!("{}", json);
            }
            OutputFormat::Table => {
                if data.is_empty() {
                    println!("{}", "No results found".dimmed());
                } else {
                    let table = Table::new(data);
                    println!("{}", table);
                }
            }
        }
        Ok(())
    }

    pub fn print_message(&self, message: &str) {
        if !self.quiet {
            println!("{}", message);
        }
    }

    pub fn print_success_message(&self, message: &str) {
        if !self.quiet {
            println!("{}", message.green());
        }
    }

    pub fn print_error(&self, error: &str) {
        eprintln!("{}: {}", "Error".red().bold(), error);
    }

    pub fn print_warning(&self, warning: &str) {
        if !self.quiet {
            eprintln!("{}: {}", "Warning".yellow().bold(), warning);
        }
    }
}