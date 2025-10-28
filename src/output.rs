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
