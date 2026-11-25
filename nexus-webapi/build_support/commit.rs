pub fn commit_hash(
    from_env: Option<&str>,
    from_git: impl FnOnce() -> Option<String>,
) -> Option<String> {
    if let Some(hash) = from_env.map(str::trim).filter(|hash| !hash.is_empty()) {
        return Some(hash.to_string());
    }
    from_git().and_then(|hash| {
        let hash = hash.trim();
        (!hash.is_empty()).then(|| hash.to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::commit_hash;

    #[test]
    fn a_nonempty_environment_value_wins_without_reading_git() {
        let hash = commit_hash(Some(" abc123 "), || panic!("git must not be read"));
        assert_eq!(hash.as_deref(), Some("abc123"));
    }

    #[test]
    fn an_empty_environment_value_falls_back_to_git() {
        for from_env in [None, Some(""), Some("  \t")] {
            let hash = commit_hash(from_env, || Some("git123".to_string()));
            assert_eq!(hash.as_deref(), Some("git123"));
        }
    }

    #[test]
    fn no_environment_or_git_value_means_no_hash() {
        assert_eq!(commit_hash(None, || None), None);
    }

    #[test]
    fn git_output_is_trimmed() {
        let hash = commit_hash(None, || Some("abc123\n".to_string()));
        assert_eq!(hash.as_deref(), Some("abc123"));
    }
}
