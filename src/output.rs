//! Output formatting for pgqrs CLI.
//!
//! This module provides output writers for formatting command results in different formats.
//!
//! ## What
//!
//! - [`OutputWriter`] enum handles different output formats (JSON, Table)
//! - [`JsonOutputWriter`] serializes results to JSON
//! - [`TableOutputWriter`] displays results in human-readable tables
//!
//! ## How
//!
//! Use the appropriate writer based on user preference for displaying CLI command results.
//!
//! ### Example
//!
//! ```rust
//! use pgqrs::output::{OutputWriter, JsonOutputWriter};
//! let writer = OutputWriter::Json(JsonOutputWriter);
//! // writer.write_list(&results, &mut output)?;
//! ```

use serde::Serialize;
use tabled::{Table, Tabled};

pub enum OutputWriter {
    /// Display results in a human-readable table
    Table(TableOutputWriter),
    /// Display results as JSON
    Json(JsonOutputWriter),
}

impl OutputWriter {
    /// Write a list of items using the configured output format.
    ///
    /// # Arguments
    /// * `items` - Slice of items to write (must implement Serialize + Tabled)
    /// * `out` - Output writer destination
    ///
    /// # Returns
    /// Ok if writing succeeded, error otherwise.
    pub fn write_list<T: Serialize + Tabled>(
        &self,
        items: &[T],
        out: &mut dyn std::io::Write,
    ) -> anyhow::Result<()> {
        match self {
            OutputWriter::Table(writer) => writer.write_list(items, out),
            OutputWriter::Json(writer) => writer.write_list(items, out),
        }
    }

    /// Write a single item using the configured output format.
    ///
    /// # Arguments
    /// * `item` - Item to write (must implement Serialize + Tabled)
    /// * `out` - Output writer destination
    /// # Returns
    /// Ok if writing succeeded, error otherwise.
    pub fn write_item<T: Serialize + Tabled>(
        &self,
        item: &T,
        out: &mut dyn std::io::Write,
    ) -> anyhow::Result<()> {
        match self {
            OutputWriter::Table(writer) => writer.write_item(item, out),
            OutputWriter::Json(writer) => writer.write_item(item, out),
        }
    }
}

/// Writer for formatting output as human-readable tables
pub struct TableOutputWriter;
impl TableOutputWriter {
    /// Write items as a formatted table.
    ///
    /// # Arguments
    /// * `items` - Slice of items to format as table rows
    /// * `out` - Output writer destination
    ///
    /// # Returns
    /// Ok if writing succeeded, error otherwise.
    pub fn write_list<T: Serialize + Tabled>(
        &self,
        items: &[T],
        out: &mut dyn std::io::Write,
    ) -> anyhow::Result<()> {
        let table = Table::new(items);
        writeln!(out, "{}", table)?;
        Ok(())
    }

    /// Write a single item as a formatted table.
    ///
    /// # Arguments
    /// * `item` - A single item to format as a table row
    /// * `out` - Output writer destination
    /// # Returns
    /// Ok if writing succeeded, error otherwise.
    pub fn write_item<T: Serialize + Tabled>(
        &self,
        items: &T,
        out: &mut dyn std::io::Write,
    ) -> anyhow::Result<()> {
        self.write_list(std::slice::from_ref(items), out)
    }
}

/// Writer for formatting output as JSON
pub struct JsonOutputWriter;
impl JsonOutputWriter {
    /// Write items as pretty-printed JSON.
    ///
    /// # Arguments
    /// * `items` - Slice of items to serialize as JSON
    /// * `out` - Output writer destination
    ///
    /// # Returns
    /// Ok if writing succeeded, error otherwise.
    pub fn write_list<T: Serialize>(
        &self,
        items: &[T],
        out: &mut dyn std::io::Write,
    ) -> anyhow::Result<()> {
        let json = serde_json::to_string_pretty(items)?;
        writeln!(out, "{}", json)?;
        Ok(())
    }

    /// Write a single item as pretty-printed JSON.
    ///
    /// # Arguments
    /// * `items` - Slice containing the single item to serialize as JSON
    /// * `out` - Output writer destination
    /// # Returns
    /// Ok if writing succeeded, error otherwise.
    pub fn write_item<T: Serialize>(
        &self,
        item: &T,
        out: &mut dyn std::io::Write,
    ) -> anyhow::Result<()> {
        let json = serde_json::to_string_pretty(item)?;
        writeln!(out, "{}", json)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{JsonOutputWriter, OutputWriter, TableOutputWriter};
    use chrono::Utc;
    use serde::Serialize;
    use serde_json::{json, Value};
    use tabled::Tabled;

    #[derive(Serialize, Tabled)]
    struct QueueMessage {
        pub msg_id: i64,
        pub message: Value,
        pub enqueued_at: chrono::DateTime<chrono::Utc>,
        pub read_ct: i32,
        pub vt: chrono::DateTime<chrono::Utc>,
    }

    fn sample_message() -> QueueMessage {
        QueueMessage {
            msg_id: 1,
            message: json!({"foo": "bar"}),
            enqueued_at: Utc::now(),
            read_ct: 2,
            vt: Utc::now(),
        }
    }

    #[test]
    fn test_json_writer_list() {
        let writer = OutputWriter::Json(JsonOutputWriter);
        let mut cursor = std::io::Cursor::new(Vec::new());
        let messages = vec![sample_message()];
        writer.write_list(&messages, &mut cursor).unwrap();
        let output = String::from_utf8(cursor.into_inner()).unwrap();
        assert!(output.contains("foo"));
        assert!(output.contains("msg_id"));
    }

    #[test]
    fn test_table_writer_list() {
        let writer = OutputWriter::Table(TableOutputWriter);
        let mut cursor = std::io::Cursor::new(Vec::new());
        let messages = vec![sample_message()];
        writer.write_list(&messages, &mut cursor).unwrap();
        let output = String::from_utf8(cursor.into_inner()).unwrap();

        // Verify table format and content
        assert!(
            output.contains("msg_id"),
            "Should contain msg_id column header"
        );
        assert!(
            output.contains("message"),
            "Should contain message column header"
        );
        assert!(
            output.contains("foo"),
            "Should contain the foo key from JSON"
        );
        assert!(
            output.contains("bar"),
            "Should contain the bar value from JSON"
        );

        // Verify table structure (borders and separators)
        assert!(
            output.contains("+"),
            "Should contain table corner characters"
        );
        assert!(
            output.contains("|"),
            "Should contain table border characters"
        );
        assert!(
            output.contains("-"),
            "Should contain table horizontal lines"
        );
    }

    #[test]
    fn test_json_writer_item() {
        let writer = OutputWriter::Json(JsonOutputWriter);
        let mut cursor = std::io::Cursor::new(Vec::new());
        let message = sample_message();
        writer.write_item(&message, &mut cursor).unwrap();
        let output = String::from_utf8(cursor.into_inner()).unwrap();
        assert!(output.contains("foo"));
        assert!(output.contains("msg_id"));
    }
}
