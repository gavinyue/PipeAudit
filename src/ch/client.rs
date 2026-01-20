use anyhow::{Context, Result};
use clickhouse::Client;
use serde::Deserialize;

/// ClickHouse HTTP client wrapper
#[derive(Clone)]
pub struct ChClient {
    client: Client,
    endpoint: String,
}

impl ChClient {
    /// Create a new ClickHouse client
    pub fn new(endpoint: &str, user: &str, password: &str, database: &str) -> Self {
        let client = Client::default()
            .with_url(endpoint)
            .with_user(user)
            .with_password(password)
            .with_database(database);

        Self {
            client,
            endpoint: endpoint.to_string(),
        }
    }

    /// Test connection with SELECT 1
    pub async fn ping(&self) -> Result<()> {
        #[derive(Debug, Deserialize, clickhouse::Row)]
        struct PingResult {
            result: u8,
        }

        let result: PingResult = self
            .client
            .query("SELECT 1 AS result")
            .fetch_one()
            .await
            .with_context(|| format!("Failed to connect to ClickHouse at {}", self.endpoint))?;

        anyhow::ensure!(
            result.result == 1,
            "Unexpected ping result: {}",
            result.result
        );

        Ok(())
    }

    /// Execute a query and return deserialized rows
    pub async fn fetch_all<T>(&self, sql: &str) -> Result<Vec<T>>
    where
        T: clickhouse::Row + for<'a> Deserialize<'a>,
    {
        let rows = self
            .client
            .query(sql)
            .fetch_all()
            .await
            .with_context(|| format!("Failed to execute query: {}", sql))?;

        Ok(rows)
    }

    /// Execute a query and return a single row
    pub async fn fetch_one<T>(&self, sql: &str) -> Result<T>
    where
        T: clickhouse::Row + for<'a> Deserialize<'a>,
    {
        let row = self
            .client
            .query(sql)
            .fetch_one()
            .await
            .with_context(|| format!("Failed to execute query: {}", sql))?;

        Ok(row)
    }

    /// Execute a query and return optional single row
    pub async fn fetch_optional<T>(&self, sql: &str) -> Result<Option<T>>
    where
        T: clickhouse::Row + for<'a> Deserialize<'a>,
    {
        let row = self
            .client
            .query(sql)
            .fetch_optional()
            .await
            .with_context(|| format!("Failed to execute query: {}", sql))?;

        Ok(row)
    }

    /// Get the endpoint URL
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_creation() {
        let client = ChClient::new(
            "http://localhost:8123",
            "default",
            "password",
            "testdb",
        );
        assert_eq!(client.endpoint(), "http://localhost:8123");
    }

    #[test]
    fn test_client_clone() {
        let client = ChClient::new(
            "http://localhost:8123",
            "default",
            "",
            "default",
        );
        let cloned = client.clone();
        assert_eq!(cloned.endpoint(), client.endpoint());
    }
}
