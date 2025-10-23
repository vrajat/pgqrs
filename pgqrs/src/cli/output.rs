use clap::ValueEnum;
use colored::Colorize;
use serde::Serialize;
use tabled::{Table, Tabled};

#[derive(Debug, Clone, ValueEnum)]
pub enum OutputFormat {
    /// Display results in a human-readable table
    Table,
    /// Display results as JSON
    Json,
}

impl std::fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutputFormat::Table => write!(f, "table"),
            OutputFormat::Json => write!(f, "json"),
        }
    }
}

impl std::str::FromStr for OutputFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "table" => Ok(OutputFormat::Table),
            "json" => Ok(OutputFormat::Json),
            _ => Err(format!("Invalid output format: {}", s)),
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

    pub fn print_list<T>(&self, items: &[T]) -> anyhow::Result<()>
    where
        T: Serialize + Tabled,
    {
        if self.quiet {
            return Ok(());
        }

        match self.format {
            OutputFormat::Table => {
                if items.is_empty() {
                    println!("No items found");
                } else {
                    let table = Table::new(items);
                    println!("{}", table);
                }
            }
            OutputFormat::Json => {
                let json = serde_json::to_string_pretty(items)?;
                println!("{}", json);
            }
        }

        Ok(())
    }

    pub fn print_success<T>(&self, item: &T) -> anyhow::Result<()>
    where
        T: Serialize + Tabled,
    {
        if self.quiet {
            return Ok(());
        }

        match self.format {
            OutputFormat::Table => {
                let table = Table::new(&[item]);
                println!("{}", table);
            }
            OutputFormat::Json => {
                let json = serde_json::to_string_pretty(item)?;
                println!("{}", json);
            }
        }

        Ok(())
    }

    pub fn print_success_message(&self, message: &str) {
        if !self.quiet {
            println!("{}", message.green());
        }
    }

    pub fn print_error(&self, message: &str) {
        eprintln!("{}", message.red());
    }
}
