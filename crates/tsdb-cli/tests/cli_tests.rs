use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn test_cli_help_output() {
    let mut cmd = Command::cargo_bin("tsdb-cli").unwrap();
    cmd.arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("status"))
        .stdout(predicate::str::contains("query"))
        .stdout(predicate::str::contains("compact"))
        .stdout(predicate::str::contains("bench"))
        .stdout(predicate::str::contains("import"))
        .stdout(predicate::str::contains("archive"));
}

#[test]
fn test_cli_version_output() {
    let mut cmd = Command::cargo_bin("tsdb-cli").unwrap();
    cmd.arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains("tsdb-cli"));
}

#[test]
fn test_cli_status_output() {
    let dir = tempfile::tempdir().unwrap();
    let mut cmd = Command::cargo_bin("tsdb-cli").unwrap();
    cmd.arg("status")
        .arg("--data-dir")
        .arg(dir.path())
        .assert()
        .success()
        .stdout(predicate::str::contains("TSDB Status"));
}

#[test]
fn test_cli_compact_success() {
    let dir = tempfile::tempdir().unwrap();
    let mut cmd = Command::cargo_bin("tsdb-cli").unwrap();
    cmd.arg("compact")
        .arg("--data-dir")
        .arg(dir.path())
        .assert()
        .success();
}

#[test]
fn test_cli_doctor_output() {
    let dir = tempfile::tempdir().unwrap();
    let mut cmd = Command::cargo_bin("tsdb-cli").unwrap();
    cmd.arg("doctor")
        .arg("--data-dir")
        .arg(dir.path())
        .assert()
        .success()
        .stdout(predicate::str::contains("Doctor"));
}

#[test]
fn test_cli_bench_write() {
    let dir = tempfile::tempdir().unwrap();
    let mut cmd = Command::cargo_bin("tsdb-cli").unwrap();
    cmd.arg("bench")
        .arg("--mode")
        .arg("write")
        .arg("--data-dir")
        .arg(dir.path())
        .arg("--points")
        .arg("1000")
        .assert()
        .success()
        .stdout(predicate::str::contains("Write Benchmark"));
}

#[test]
fn test_cli_archive_list_empty() {
    let dir = tempfile::tempdir().unwrap();
    let archive_dir = tempfile::tempdir().unwrap();
    let mut cmd = Command::cargo_bin("tsdb-cli").unwrap();
    cmd.arg("archive")
        .arg("list")
        .arg("--data-dir")
        .arg(dir.path())
        .arg("--archive-dir")
        .arg(archive_dir.path())
        .assert()
        .success();
}
