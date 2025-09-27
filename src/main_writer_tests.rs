#[cfg(test)]
mod tests {
    use crate::{CsvOutputWriter, JsonOutputWriter, OutputFormatWriter, YamlOutputWriter};
    use chrono::Utc;
    use pgqrs::types::QueueMessage;
    use serde_json::json;

    fn sample_message() -> QueueMessage {
        QueueMessage {
            msg_id: 1,
            enqueued_at: Utc::now(),
            read_ct: 2,
            vt: Utc::now(),
            message: json!({"foo": "bar"}),
        }
    }

    #[test]
    fn test_json_writer() {
        let writer = OutputFormatWriter::Json(JsonOutputWriter);
        let mut cursor = std::io::Cursor::new(Vec::new());
        let messages = vec![sample_message()];
        writer.write(&messages, &mut cursor).unwrap();
        let output = String::from_utf8(cursor.into_inner()).unwrap();
        assert!(output.contains("foo"));
        assert!(output.contains("msg_id"));
    }

    #[test]
    fn test_csv_writer() {
        let writer = CsvOutputWriter;
        let mut cursor = std::io::Cursor::new(Vec::new());
        let messages = vec![sample_message()];
        writer.write_queue_messages(&messages, &mut cursor).unwrap();
        let output = String::from_utf8(cursor.into_inner()).unwrap();
        assert!(output.contains("msg_id,enqueued_at,read_ct,vt,message"));
        assert!(output.contains("bar"));
    }

    #[test]
    fn test_yaml_writer() {
        let writer = OutputFormatWriter::Yaml(YamlOutputWriter);
        let mut cursor = std::io::Cursor::new(Vec::new());
        let messages = vec![sample_message()];
        writer.write(&messages, &mut cursor).unwrap();
        let output = String::from_utf8(cursor.into_inner()).unwrap();
        assert!(output.contains("foo: bar"));
        assert!(output.contains("msg_id:"));
    }
}
