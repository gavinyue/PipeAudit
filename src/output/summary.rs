use crate::report::{Report, ReportStatus, Severity};

/// Print human-readable summary to stdout
pub fn print_summary(report: &Report, output_path: &str) {
    println!();
    println!("╭───────────────────────────────────────────────────────────────╮");
    println!("│                  PipeAudit Report Summary                     │");
    println!("╰───────────────────────────────────────────────────────────────╯");
    println!();

    // Target info
    println!(
        "Target: {} / {}",
        report.targets.endpoint, report.targets.database
    );
    println!("Tables: {}", report.targets.tables.join(", "));
    println!("Generated: {}", report.generated_at);
    println!();

    // Status
    let (status_icon, status_text) = match report.summary.status {
        ReportStatus::Healthy => ("✅", "HEALTHY"),
        ReportStatus::Warning => ("⚠️ ", "WARNING"),
        ReportStatus::Critical => ("🚨", "CRITICAL"),
    };
    println!("Status: {} {}", status_icon, status_text);
    println!();

    // Findings
    if !report.findings.is_empty() {
        println!(
            "Findings ({} total, {} critical, {} warning):",
            report.summary.findings_count,
            report.summary.critical_count,
            report.summary.warning_count
        );

        for finding in &report.findings {
            let icon = match finding.severity {
                Severity::Critical => "🚨",
                Severity::Warning => "⚠️ ",
            };
            println!(
                "  {} [{}] {}: {}",
                icon, finding.rule_id, finding.target, finding.message
            );
        }
        println!();
    }

    // Actions
    if !report.actions.is_empty() {
        println!("Recommended Actions:");
        for (i, action) in report.actions.iter().enumerate() {
            println!(
                "  {}. [{:?}] {}",
                i + 1,
                action.priority,
                action.description
            );
            if let Some(sql) = &action.sql {
                println!("     SQL: {}", truncate(sql, 60));
            }
        }
        println!();
    }

    // MV DAG summary
    if let Some(dag) = &report.sections.mv_dag {
        println!(
            "MV DAG: {} tables, {} materialized views, max depth {}",
            dag.total_tables, dag.total_mvs, dag.max_depth
        );
        println!();
    }

    println!("Full report written to: {}", output_path);
    println!();
}

fn truncate(s: &str, max_len: usize) -> String {
    if s.len() > max_len {
        format!("{}...", &s[..max_len - 3])
    } else {
        s.to_string()
    }
}

/// Format summary as string (for testing)
pub fn format_summary(report: &Report) -> String {
    let mut output = String::new();

    output.push_str(&format!(
        "Status: {:?}\n",
        report.summary.status
    ));
    output.push_str(&format!(
        "Findings: {}\n",
        report.summary.findings_count
    ));

    for finding in &report.findings {
        output.push_str(&format!(
            "[{:?}] {}: {}\n",
            finding.severity, finding.rule_id, finding.message
        ));
    }

    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report::{Finding, Report, Targets};

    fn test_report() -> Report {
        Report::new(Targets {
            endpoint: "http://localhost:8123".to_string(),
            database: "testdb".to_string(),
            tables: vec!["events".to_string()],
        })
    }

    #[test]
    fn test_format_summary_healthy() {
        let report = test_report();
        let output = format_summary(&report);

        assert!(output.contains("Healthy"));
        assert!(output.contains("Findings: 0"));
    }

    #[test]
    fn test_format_summary_with_findings() {
        let mut report = test_report();
        report.findings.push(Finding {
            id: "f-1".to_string(),
            rule_id: "parts_explosion".to_string(),
            severity: Severity::Warning,
            target: "testdb.events".to_string(),
            message: "Too many parts".to_string(),
            evidence_refs: vec![],
            confidence: 1.0,
        });
        report.summary.findings_count = 1;
        report.summary.warning_count = 1;
        report.summary.status = ReportStatus::Warning;

        let output = format_summary(&report);

        assert!(output.contains("Warning"));
        assert!(output.contains("parts_explosion"));
    }

    #[test]
    fn test_truncate() {
        assert_eq!(truncate("short", 10), "short");
        assert_eq!(truncate("this is a long string", 10), "this is...");
    }
}
