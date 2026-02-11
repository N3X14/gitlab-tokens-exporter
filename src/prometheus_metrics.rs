//! Generates the prometheus metrics

use core::error::Error;
use core::fmt::Write as _;
use tracing::{instrument};
use crate::gitlab::Token;

#[expect(clippy::arithmetic_side_effects, reason = "Not handled by chrono")]
#[instrument(err, skip_all)]
pub fn build(gitlab_token: &Token) -> Result<String, Box<dyn Error + Send + Sync>> {
    let mut res = String::new();
    let date_now = chrono::Utc::now().date_naive();

    let token_type = match *gitlab_token {
        Token::Group { .. } => "group",
        Token::Project { .. } => "project",
        Token::User { .. } => "user",
    };

    let (name, active, _revoked, expires_at, full_path) = match *gitlab_token {
        Token::Group { ref token, ref full_path, .. } |
        Token::Project { ref token, ref full_path, .. } => (
            &token.name,
            token.active,
            token.revoked,
            token.expires_at,
            full_path,
        ),
        Token::User { ref token, ref full_path } => (
            &token.name,
            token.active,
            token.revoked,
            token.expires_at,
            full_path,
        ),
    };

    let metric_name = "gitlab_token_expiration";
    let days_to_expiration = match expires_at {
        Some(expires_at) => (expires_at - date_now).num_days() as f64,
        None => f64::NAN,
    };

    writeln!(res, "# HELP {metric_name} Days before GitLab token expires")?;
    writeln!(res, "# TYPE {metric_name} gauge")?;

    write!(
        res,
        "{metric_name}{{token_type=\"{token_type}\",full_path=\"{full_path}\",token_name=\"{name}\",active=\"{active}\"}} {}\n",
        days_to_expiration
    )?;

    Ok(res)
}
