use crate::report::Report;
use anyhow::{Context, Result};
use std::path::Path;

/// Write report to JSON file
pub fn write_report(report: &Report, path: &Path) -> Result<()> {
    let json = serde_json::to_string_pretty(report)
        .context("Failed to serialize report to JSON")?;

    std::fs::write(path, &json)
        .with_context(|| format!("Failed to write report to {:?}", path))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report::{Report, ReportStatus, Summary, Targets};
    use tempfile::NamedTempFile;

    fn test_report() -> Report {
        let mut report = Report::new(Targets {
            endpoint: "http://localhost:8123".to_string(),
            database: "testdb".to_string(),
            tables: vec!["events".to_string()],
        });
        report.report_id = "test-id".to_string();
        report
    }

    #[test]
    fn test_write_report_creates_file() {
        let report = test_report();
        let temp = NamedTempFile::new().unwrap();

        write_report(&report, temp.path()).unwrap();

        assert!(temp.path().exists());
        let content = std::fs::read_to_string(temp.path()).unwrap();
        assert!(!content.is_empty());
    }

    #[test]
    fn test_write_report_valid_json() {
        let report = test_report();
        let temp = NamedTempFile::new().unwrap();

        write_report(&report, temp.path()).unwrap();

        let content = std::fs::read_to_string(temp.path()).unwrap();
        let parsed: Report = serde_json::from_str(&content).unwrap();

        assert_eq!(report.report_id, parsed.report_id);
    }

    #[test]
    fn test_write_report_pretty_printed() {
        let report = test_report();
        let temp = NamedTempFile::new().unwrap();

        write_report(&report, temp.path()).unwrap();

        let content = std::fs::read_to_string(temp.path()).unwrap();
        // Pretty printed JSON has newlines
        assert!(content.contains('\n'));
    }
}
