//! This is the main actor, it handles all [Message]

use core::error::Error;
use dotenv::dotenv;
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::env;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tracing::{debug, error, info, instrument, warn};

use crate::gitlab::{Group, OffsetBasedPagination as _, Token, get_group_full_path};
use crate::{gitlab, prometheus_metrics};

/// Defines possible states
#[derive(Clone, Debug)]
pub enum ActorState {
    /// Stores an error string if [`gitlab_get_data`] fails
    Error(String),
    /// Stores the string that is returned when requesting `/metrics`
    Loaded(String),
    /// First state when the program starts
    Loading,
    /// Used when no token has been found
    NoToken,
}

/// Defines the messages handled by the state actor
#[derive(Debug)]
pub enum Message {
    /// Get the state and send it to `respond_to`
    Get {
        /// Channel we have to send the state to
        respond_to: oneshot::Sender<ActorState>,
    },
    /// This message is sent by the update task when it finishes
    Set(Result<String, String>),
    /// This message is only send by the [timer](crate::timer) actor
    Update,
}

/// Handles `send()` result by dismissing it ;)
async fn send_msg(sender: mpsc::Sender<Message>, msg: Message) {
    match sender.send(msg).await {
        Ok(send_res) => send_res,
        Err(err) => {
            // We can't do anything at this point. If this fails, we're in bad shape :(
            error!("Failed to send a message: {err}");
        }
    }
}

#[instrument(skip_all)]
/// Handles [Message::Update] messages
///
/// When finished, it sends its result by sending Message::Set to the main actor
async fn gitlab_get_data(
    hostname: String,
    gitlab_token: String,
    accept_invalid_certs: bool,
    owned_entities_only: bool,
    sender: mpsc::Sender<Message>,
) {
    info!("starting...");

    // Parse filter set from env
    let filter_set: Option<HashSet<String>> = env::var("GITLAB_FILTER")
        .ok()
        .map(|val| val.split(',')
            .map(|s| s.trim().to_ascii_lowercase())
            .filter(|s| !s.is_empty())
            .collect());

    // Create an HTTP client
    let http_client = match reqwest::ClientBuilder::new()
        .danger_accept_invalid_certs(accept_invalid_certs)
        .build()
    {
        Ok(res) => res,
        Err(err) => {
            error!("{err}");
            send_msg(sender, Message::Set(Err(format!("{err:?}")))).await;
            return;
        }
    };

    // We are going to spawn 2 tasks to speed up the data collection
    // One to get the projects tokens
    // One to get the groups tokens
    // The users tokens are handled differently, because it can fail but it should *not* be a hard failure.
    let mut join_set: JoinSet<Result<String, Box<dyn Error + Send + Sync>>> = JoinSet::new();

    // Get tokens for all projects
    let http_client_clone1 = http_client.clone();
    let hostname_clone1 = hostname.clone();
    let gitlab_token_clone1 = gitlab_token.clone();
    let filter_set_clone1 = filter_set.clone();
    let owned_entities_only_clone1 = owned_entities_only;
    join_set.spawn(async move {
        let mut res = String::new();
        let mut url = format!(
            "https://{hostname_clone1}/api/v4/projects?per_page=100&archived=false{}",
            if owned_entities_only_clone1 {
                format!("&min_access_level={}", gitlab::AccessLevel::Owner as u8)
            } else {
                String::new()
            }
        );
        let projects =
            gitlab::Project::get_all(&http_client_clone1, url, &gitlab_token_clone1).await?;
        for project in projects {
            let name = project.path_with_namespace.to_ascii_lowercase();
            if let Some(ref filter) = filter_set_clone1 {
                if !filter.contains(&name) {
                    continue;
                }
            }
            url = format!(
                "https://{hostname_clone1}/api/v4/projects/{}/access_tokens?per_page=100",
                project.id
            );
            let project_tokens =
                gitlab::AccessToken::get_all(&http_client_clone1, url, &gitlab_token_clone1)
                    .await?;
            for project_token in project_tokens {
                let token = Token::Project {
                    token: project_token,
                    full_path: project.path_with_namespace.clone(),
                    web_url: project.web_url.clone(),
                };
                let token_metric_str = prometheus_metrics::build(&token)?;
                res.push_str(&token_metric_str);
            }
        }
        Ok(res)
    });

    // Get tokens for all groups
    let http_client_clone2 = http_client.clone();
    let hostname_clone2 = hostname.clone();
    let gitlab_token_clone2 = gitlab_token.clone();
    let filter_set_clone2 = filter_set.clone();
    let owned_entities_only_clone2 = owned_entities_only;
    join_set.spawn(async move {
        let mut group_id_cache: HashMap<usize, Group> = HashMap::new();
        let mut res = String::new();
        let mut url = format!(
            "https://{hostname_clone2}/api/v4/groups?per_page=100&archived=false{}",
            if owned_entities_only_clone2 {
                format!("&min_access_level={}", gitlab::AccessLevel::Owner as u8)
            } else {
                String::new()
            }
        );
        let groups = gitlab::Group::get_all(&http_client_clone2, url, &gitlab_token_clone2).await?;
        for group in groups {
            let name = group.path.to_ascii_lowercase();
            if let Some(ref filter) = filter_set_clone2 {
                if !filter.contains(&name) {
                    continue;
                }
            }
            url = format!(
                "https://{hostname_clone2}/api/v4/groups/{}/access_tokens?per_page=100",
                group.id
            );
            let group_tokens =
                gitlab::AccessToken::get_all(&http_client_clone2, url, &gitlab_token_clone2)
                    .await?;
            for group_token in group_tokens {
                let token = Token::Group {
                    token: group_token,
                    full_path: get_group_full_path(
                        &http_client_clone2,
                        &hostname_clone2,
                        &gitlab_token_clone2,
                        &group,
                        &mut group_id_cache,
                    )
                    .await?,
                    web_url: group.web_url.clone(),
                };
                let token_metric_str = prometheus_metrics::build(&token)?;
                res.push_str(&token_metric_str);
            }
        }
        Ok(res)
    });

    // Waiting for all our async tasks
    let task_outputs = join_set.join_all().await;
    let mut return_value = String::new();
    for task_output in task_outputs {
        match task_output {
            Ok(value) => return_value.push_str(&value),
            Err(err) => {
                let msg = format!("Failed to get tokens in async task: {err:?}");
                error!(msg);
                send_msg(sender, Message::Set(Err(msg))).await;
                return;
            }
        }
    }

    // Get tokens for all users
    let mut res = String::new();
    let mut url = format!("https://{hostname}/api/v4/users?per_page=100");
    let filter_set_clone3 = filter_set.clone();
    let user_tokens: Result<String, Box<dyn Error + Send + Sync>> = async {
        let current_user = gitlab::get_current_user(&http_client, &hostname, &gitlab_token).await?;
        if current_user.is_admin {
            let users = gitlab::User::get_all(&http_client, url, &gitlab_token).await?;
            let human_users_re = Regex::new("(project|group)_[0-9]+_bot_[0-9a-f]{32,}")?;
            let user_ids: HashMap<_, _> = users
                .iter()
                .filter(|user| !human_users_re.is_match(&user.username))
                .map(|user| (user.id, user.username.clone()))
                .collect();

            // Get all personnal access tokens
            url = format!("https://{hostname}/api/v4/personal_access_tokens?per_page=100");
            let mut personnal_access_tokens =
                gitlab::PersonalAccessToken::get_all(&http_client, url, &gitlab_token).await?;

            // Retain personnal access tokens of human users
            personnal_access_tokens.retain(|pat| user_ids.contains_key(&pat.user_id));

            for personnal_access_token in personnal_access_tokens {
                let username = user_ids
                    .get(&personnal_access_token.user_id)
                    .map_or("", |val| val);

                // Filtering
                let name = username.to_ascii_lowercase();
                if let Some(ref filter) = filter_set_clone3 {
                    if !filter.contains(&name) {
                        continue;
                    }
                }

                let token_str = prometheus_metrics::build(&Token::User {
                    token: personnal_access_token,
                    full_path: username.to_owned()
                })?;
                res.push_str(&token_str);
            }
            Ok(res)
        } else {
            let msg =
                "Can't get users tokens with the current GITLAB_TOKEN (current_user.is_admin == false)";
            warn!("{msg}");
            Ok(String::new())
        }
    }
    .await;

    match user_tokens {
        Ok(value) => return_value.push_str(&value),
        Err(err) => {
            let msg = format!("Failed to get tokens for all users: {err:?}");
            error!(msg);
            send_msg(sender, Message::Set(Err(msg))).await;
            return;
        }
    }

    send_msg(sender, Message::Set(Ok(return_value))).await;
    info!("done");
}

#[instrument(skip_all)]
/// Main actor, receives all [Message]
pub async fn gitlab_tokens_actor(
    mut receiver: mpsc::Receiver<Message>,
    sender: mpsc::Sender<Message>,
) {
    let mut state = ActorState::Loading;

    let _res = dotenv();

    let Ok(token) = env::var("GITLAB_TOKEN") else {
        error!("env variable GITLAB_TOKEN is not defined");
        return;
    };

    let Ok(hostname) = env::var("GITLAB_HOSTNAME") else {
        error!("env variable GITLAB_HOSTNAME is not defined");
        return;
    };

    // Checking ACCEPT_INVALID_CERTS env variable
    let accept_invalid_cert = match env::var("ACCEPT_INVALID_CERTS") {
        Ok(value) => {
            if value == "yes" {
                true
            } else {
                error!(
                    "The environment variable 'ACCEPT_INVALID_CERTS' is set, but not to its only possible value : 'yes'"
                );
                return;
            }
        }
        Err(_) => false,
    };

    // Checking OWNED_ENTITIES_ONLY env variable...
    let owned_entities_only = match env::var("OWNED_ENTITIES_ONLY") {
        Ok(value) => {
            if value == "yes" {
                true
            } else {
                error!(
                    "The environment variable 'OWNED_ENTITIES_ONLY' is set, but not to its only possible value : 'yes'"
                );
                return;
            }
        }
        Err(_) => false,
    };

    // We now wait for some messages
    loop {
        let msg = receiver.recv().await;
        if let Some(msg_value) = msg {
            match msg_value {
                Message::Get { respond_to } => {
                    debug!("received Message::Get");
                    respond_to.send(state.clone()).unwrap_or_else(|_| {
                        warn!("Failed to send reponse : oneshot channel was closed");
                    });
                }
                Message::Update => {
                    // We are going to spawn a async task to get the data from gitlab.
                    // This task will send us Message::Set with the result to
                    // update our 'state' variable
                    debug!("received Message::Update");
                    tokio::spawn(gitlab_get_data(
                        hostname.clone(),
                        token.clone(),
                        accept_invalid_cert,
                        owned_entities_only,
                        sender.clone(),
                    ));
                }
                Message::Set(gitlab_data) => {
                    debug!("received Message::Set");
                    match gitlab_data {
                        Ok(data) => {
                            if data.is_empty() {
                                warn!("No token has been found");
                                state = ActorState::NoToken;
                            } else {
                                state = ActorState::Loaded(data);
                            }
                        }
                        Err(err) => state = ActorState::Error(err),
                    }
                }
            }
        } else {
            error!("recv failed");
            break;
        }
    }
}
