#[path = "build_support/commit.rs"]
mod commit;

use std::process::Command;

fn git_commit_hash() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout).ok()
}

fn main() {
    println!("cargo:rerun-if-env-changed=GIT_COMMIT");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=build_support");
    println!("cargo:rerun-if-changed=src");

    let from_env = std::env::var("GIT_COMMIT").ok();
    if let Some(hash) = commit::commit_hash(from_env.as_deref(), git_commit_hash) {
        println!("cargo:rustc-env=GIT_COMMIT_HASH={hash}");
    }
}
