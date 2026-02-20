#[derive(Debug, Clone)]
pub enum QueryParam {
    I64(i64),
    I32(i32),
    String(String),
    Json(serde_json::Value),
    DateTime(Option<chrono::DateTime<chrono::Utc>>),
}

#[derive(Debug, Clone)]
pub struct QueryBuilder {
    sql: String,
    params: Vec<QueryParam>,
}

impl QueryBuilder {
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            params: Vec::new(),
        }
    }

    pub fn bind_i64(mut self, value: i64) -> Self {
        self.params.push(QueryParam::I64(value));
        self
    }

    pub fn bind_i32(mut self, value: i32) -> Self {
        self.params.push(QueryParam::I32(value));
        self
    }

    pub fn bind_string(mut self, value: impl Into<String>) -> Self {
        self.params.push(QueryParam::String(value.into()));
        self
    }

    pub fn bind_json(mut self, value: serde_json::Value) -> Self {
        self.params.push(QueryParam::Json(value));
        self
    }

    pub fn bind_datetime(mut self, value: Option<chrono::DateTime<chrono::Utc>>) -> Self {
        self.params.push(QueryParam::DateTime(value));
        self
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }

    pub fn params(&self) -> &[QueryParam] {
        &self.params
    }
}
