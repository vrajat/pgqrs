use serde::Serialize;
use tabled::{Table, Tabled};

pub enum OutputWriter {
    /// Display results in a human-readable table
    Table(TableOutputWriter),
    /// Display results as JSON
    Json(JsonOutputWriter),
}

impl OutputWriter {
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

    pub fn write<T: Serialize + Tabled>(
        &self,
        item: &T,
        out: &mut dyn std::io::Write,
    ) -> anyhow::Result<()> {
        match self {
            OutputWriter::Table(writer) => writer.write(item, out),
            OutputWriter::Json(writer) => writer.write(item, out),
        }
    }
}

pub struct TableOutputWriter;
impl TableOutputWriter {
    pub fn write_list<T: Serialize + Tabled>(
        &self,
        items: &[T],
        out: &mut dyn std::io::Write,
    ) -> anyhow::Result<()> {
        let table = Table::new(items);
        writeln!(out, "{}", table)?;
        Ok(())
    }

    pub fn write<T: Serialize + Tabled>(
        &self,
        item: &T,
        out: &mut dyn std::io::Write,
    ) -> anyhow::Result<()> {
        let table = Table::new(vec![item]);
        writeln!(out, "{}", table)?;
        Ok(())
    }
}

pub struct JsonOutputWriter;
impl JsonOutputWriter {
    pub fn write_list<T: Serialize>(
        &self,
        items: &[T],
        out: &mut dyn std::io::Write,
    ) -> anyhow::Result<()> {
        let json = serde_json::to_string_pretty(items)?;
        writeln!(out, "{}", json)?;
        Ok(())
    }

    pub fn write<T: Serialize>(
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
    use crate::cli::{
        commands::MessageInfo,
        output::{JsonOutputWriter, OutputWriter, TableOutputWriter},
    };
    use chrono::Utc;
    use serde_json::json;

    fn sample_message() -> MessageInfo {
        MessageInfo {
            id: 1,
            queue_name: "test_queue".into(),
            payload: json!({"foo": "bar"}).to_string(),
            enqueued_at: Utc::now().to_string(),
            read_count: 2,
            visibility_timeout: Utc::now().to_string(),
        }
    }

    #[test]
    fn test_json_writer() {
        let writer = OutputWriter::Json(JsonOutputWriter);
        let mut cursor = std::io::Cursor::new(Vec::new());
        let messages = vec![sample_message()];
        writer.write_list(&messages, &mut cursor).unwrap();
        let output = String::from_utf8(cursor.into_inner()).unwrap();
        assert!(output.contains("foo"));
        assert!(output.contains("msg_id"));
    }

    #[test]
    fn test_table_writer() {
        let writer = OutputWriter::Table(TableOutputWriter);
        let mut cursor = std::io::Cursor::new(Vec::new());
        let messages = vec![sample_message()];
        writer.write_list(&messages, &mut cursor).unwrap();
        let output = String::from_utf8(cursor.into_inner()).unwrap();
        assert!(output.contains("foo: bar"));
        assert!(output.contains("msg_id:"));
    }
}
