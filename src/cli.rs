use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "pipeaudit")]
#[command(about = "Audit tool for ClickHouse data pipelines")]
#[command(version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Run audit on ClickHouse tables
    Audit(AuditArgs),
}

#[derive(Parser, Debug)]
pub struct AuditArgs {
    /// ClickHouse HTTP endpoint (e.g., http://localhost:8123)
    #[arg(long, env = "CLICKHOUSE_ENDPOINT")]
    pub endpoint: String,

    /// ClickHouse user
    #[arg(long, env = "CLICKHOUSE_USER", default_value = "default")]
    pub user: String,

    /// ClickHouse password
    #[arg(long, env = "CLICKHOUSE_PASSWORD", default_value = "")]
    pub password: String,

    /// Database to audit
    #[arg(long, short)]
    pub db: String,

    /// Tables to audit (comma-separated)
    #[arg(long, short, value_delimiter = ',')]
    pub tables: Vec<String>,

    /// Output file path for JSON report
    #[arg(long, short)]
    pub out: PathBuf,

    /// SQL file for EXPLAIN analysis (optional)
    #[arg(long)]
    pub sql_file: Option<PathBuf>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_parse_audit_command() {
        let cli = Cli::parse_from([
            "pipeaudit",
            "audit",
            "--endpoint",
            "http://localhost:8123",
            "--db",
            "testdb",
            "--tables",
            "events,users",
            "--out",
            "report.json",
        ]);

        match cli.command {
            Commands::Audit(args) => {
                assert_eq!(args.endpoint, "http://localhost:8123");
                assert_eq!(args.db, "testdb");
                assert_eq!(args.tables, vec!["events", "users"]);
                assert_eq!(args.out, PathBuf::from("report.json"));
                assert_eq!(args.user, "default");
                assert_eq!(args.password, "");
            }
        }
    }

    #[test]
    fn test_cli_parse_single_table() {
        let cli = Cli::parse_from([
            "pipeaudit",
            "audit",
            "--endpoint",
            "http://localhost:8123",
            "--db",
            "testdb",
            "--tables",
            "events",
            "--out",
            "report.json",
        ]);

        match cli.command {
            Commands::Audit(args) => {
                assert_eq!(args.tables, vec!["events"]);
            }
        }
    }

    #[test]
    fn test_cli_parse_with_credentials() {
        let cli = Cli::parse_from([
            "pipeaudit",
            "audit",
            "--endpoint",
            "http://localhost:8123",
            "--user",
            "admin",
            "--password",
            "secret",
            "--db",
            "testdb",
            "--tables",
            "events",
            "--out",
            "report.json",
        ]);

        match cli.command {
            Commands::Audit(args) => {
                assert_eq!(args.user, "admin");
                assert_eq!(args.password, "secret");
            }
        }
    }

    #[test]
    fn test_cli_parse_with_sql_file() {
        let cli = Cli::parse_from([
            "pipeaudit",
            "audit",
            "--endpoint",
            "http://localhost:8123",
            "--db",
            "testdb",
            "--tables",
            "events",
            "--out",
            "report.json",
            "--sql-file",
            "queries.sql",
        ]);

        match cli.command {
            Commands::Audit(args) => {
                assert_eq!(args.sql_file, Some(PathBuf::from("queries.sql")));
            }
        }
    }

    #[test]
    fn test_cli_parse_short_flags() {
        let cli = Cli::parse_from([
            "pipeaudit",
            "audit",
            "--endpoint",
            "http://localhost:8123",
            "-d",
            "testdb",
            "-t",
            "events,users,orders",
            "-o",
            "report.json",
        ]);

        match cli.command {
            Commands::Audit(args) => {
                assert_eq!(args.db, "testdb");
                assert_eq!(args.tables, vec!["events", "users", "orders"]);
                assert_eq!(args.out, PathBuf::from("report.json"));
            }
        }
    }
}
