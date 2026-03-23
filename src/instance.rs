use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::io::ErrorKind;
use std::ops::Deref;
use std::path::Path;
use std::process::Stdio;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};
use std::{env, fmt};

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::{mpsc, Mutex, Notify};
use tokio::{select, task};
use tracing::{debug, error, field, info, instrument, trace, warn, Instrument};

use crate::client::Client;
use crate::config::{Config, Filter};
use crate::lsp::ext::Tag;
use crate::lsp::jsonrpc::{Message, Notification, Request, RequestId, ResponseSuccess, Version};
use crate::lsp::transport::{LspReader, LspWriter};
use crate::lsp::{self, ext};

/// Specifies server configuration
///
/// If another server with the same configuration is requested we can reuse it.
#[derive(Clone, Debug)]
pub struct InstanceKey {
    pub server: String,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    match_env: BTreeMap<String, String>,
    pub workspace_root: WorkspaceRoot,
}

impl InstanceKey {
    pub fn new(
        server: String,
        args: Vec<String>,
        env: BTreeMap<String, String>,
        workspace_root: WorkspaceRoot,
        match_filters: &[Filter],
    ) -> Self {
        let match_env = build_match_env(&env, match_filters);
        Self {
            server,
            args,
            env,
            match_env,
            workspace_root,
        }
    }

    pub fn match_env(&self) -> &BTreeMap<String, String> {
        &self.match_env
    }
}

impl PartialEq for InstanceKey {
    fn eq(&self, other: &Self) -> bool {
        self.server == other.server
            && self.args == other.args
            && self.match_env == other.match_env
            && self.workspace_root == other.workspace_root
    }
}

impl Eq for InstanceKey {}

impl Hash for InstanceKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.server.hash(state);
        self.args.hash(state);
        self.match_env.hash(state);
        self.workspace_root.hash(state);
    }
}

/// Represents workspace root as a unique directory
///
/// On some file systems or operating systems file/directory equality is more
/// complicated than equality on paths. Windows is by default case-insensitive,
/// and MacOS's APFS compares after unicode normalization. The only reliable way
/// to ensure two paths are the same is to compare their inode and dev numbers.
#[derive(Clone)]
pub struct WorkspaceRoot {
    path: String,
    device_id: u64,
    file_id: u64,
}

impl WorkspaceRoot {
    #[cfg(any(target_os = "redox", unix))]
    pub fn from_path(path: String) -> Result<Self> {
        use std::os::unix::fs::MetadataExt as _;

        let metadata = Path::new(&path)
            .metadata()
            .with_context(|| format!("error getting metadata for path {path:?}"))?;

        Ok(Self {
            path,
            device_id: metadata.dev(),
            file_id: metadata.ino(),
        })
    }

    #[cfg(windows)]
    pub fn from_path(path: String) -> Result<Self> {
        use winapi_util::{file, Handle};

        let handle =
            Handle::from_path_any(&path).with_context(|| format!("error opening path {path:?}"))?;
        let information = file::information(&handle)
            .with_context(|| format!("error getting file information for path {path:?}"))?;

        Ok(Self {
            path,
            device_id: information.volume_serial_number(),
            file_id: information.file_index(),
        })
    }
}

impl PartialEq for WorkspaceRoot {
    fn eq(&self, other: &Self) -> bool {
        // Skip `self.path`
        self.device_id == other.device_id && self.file_id == other.file_id
    }
}

impl Eq for WorkspaceRoot {}

impl Hash for WorkspaceRoot {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Skip `self.path`
        self.device_id.hash(state);
        self.file_id.hash(state);
    }
}

impl fmt::Debug for WorkspaceRoot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        <String as fmt::Debug>::fmt(&self.path, f)
    }
}

/// Notification methods that represent current server state rather than one-off
/// events. The latest notification for each of these methods is cached and
/// replayed to newly connecting clients.
///
/// `experimental/serverStatus` is used by rust-analyzer (and others) to
/// advertise readiness (`quiescent: true`). Without replay, clients that
/// connect after the server is already warm never learn it's ready.
fn is_stateful_notification(method: &str) -> bool {
    matches!(method, "experimental/serverStatus")
}

/// Language server instance
pub struct Instance {
    key: InstanceKey,

    /// Language server child process id
    pid: u32,

    /// Server's response to `initialize` request
    init_result: lsp::InitializeResult,

    /// Handle for sending messages to the language server instance
    server: mpsc::Sender<Message>,

    /// Data of associated clients
    clients: Mutex<HashMap<usize, ClientData>>,

    /// Dynamic capabilities registered by the server
    dynamic_capabilities: Mutex<HashMap<String, lsp::Registration>>,

    /// Stateful server notifications cached for replay to new clients.
    ///
    /// Only the most recent notification per method is kept, matching the
    /// "last value wins" semantics of state notifications like
    /// `experimental/serverStatus`.
    cached_notifications: Mutex<HashMap<String, Notification>>,

    /// Wakes up `wait_task` and asks it to send SIGKILL to the instance.
    close: Notify,

    /// Last time a message was sent to this instance
    ///
    /// Uses seconds elapsed since an arbitrary point in the past, as returned
    /// by [`elapsed_seconds`].
    last_used: AtomicI64,
}

impl Drop for Instance {
    fn drop(&mut self) {
        // Make sure we're not leaking anything
        debug!("instance dropped");
    }
}

/// Wrapper around client handle with additional data only the server instance
/// knows about
struct ClientData {
    /// Handle for sending messages to clients
    client: Client,

    /// URIs of files currently opened by this client
    files: HashSet<String>,
}

impl ClientData {
    fn get_status(&self) -> ext::Client {
        ext::Client {
            id: self.client.id(),
            files: self.files.iter().cloned().collect(),
        }
    }
}

impl Deref for ClientData {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

// Elapsed seconds since an arbitrary [`Instant`] when this function was called for the first time
fn elapsed_seconds() -> i64 {
    static START: LazyLock<Instant> = LazyLock::new(Instant::now);
    START.elapsed().as_secs() as i64
}

fn build_match_env(
    env: &BTreeMap<String, String>,
    filters: &[Filter],
) -> BTreeMap<String, String> {
    if filters.is_empty() {
        return BTreeMap::new();
    }

    let positive: Vec<_> = filters.iter().filter(|f| !f.negative).collect();
    let negative: Vec<_> = filters.iter().filter(|f| f.negative).collect();

    env.iter()
        .filter(|(key, _)| {
            positive.iter().any(|f| f.pattern.matches(key))
                && !negative.iter().any(|f| f.pattern.matches(key))
        })
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

impl Instance {
    /// Mark the instance as used
    pub fn keep_alive(&self) {
        self.last_used.store(elapsed_seconds(), Ordering::Relaxed);
    }

    /// How many seconds is the instance idle for
    pub fn idle(&self) -> i64 {
        i64::max(
            0,
            elapsed_seconds() - self.last_used.load(Ordering::Relaxed),
        )
    }

    pub fn initialize_result(&self) -> lsp::InitializeResult {
        self.init_result.clone()
    }

    /// Add client to the instance so it can receive traffic from it
    ///
    /// Replays dynamic capabilities and cached state notifications so the new
    /// client starts with the same view of the server as existing clients.
    pub async fn add_client(&self, client: Client) {
        // NOTE: this method holds `clients`, `dynamic_capabilities`, and
        // `cached_notifications` locks across `send_message` awaits. This is
        // acceptable because the client channel is freshly created and no
        // receiver task has been spawned yet, so sends complete immediately
        // without yielding.
        //
        // If `add_client` is ever called with an active client whose channel
        // might be full, this must be restructured to snapshot and release the
        // locks first.
        let mut clients = self.clients.lock().await;
        let dyn_capabilities = self.dynamic_capabilities.lock().await;
        let cached_notifs = self.cached_notifications.lock().await;

        if !dyn_capabilities.is_empty() {
            // Register all currently cached dynamic capabilities if there are
            // any. We will drop the client response and we need to make sure
            // the request ID is unique.
            let id = RequestId::String("replay:registerCapabilities".into()).tag(Tag::Drop);
            let params = lsp::RegistrationParams {
                registrations: dyn_capabilities.values().cloned().collect(),
            };
            let req = Request {
                id,
                method: "client/registerCapability".into(),
                params: serde_json::to_value(params).unwrap(),
                jsonrpc: Version,
            };
            debug!(?req, "replaying server request");
            let _ = client.send_message(req.into()).await;
        }

        for notif in cached_notifs.values() {
            debug!(method = %notif.method, "replaying cached notification");
            let _ = client.send_message(notif.clone().into()).await;
        }

        let client = ClientData {
            client,
            files: HashSet::new(),
        };
        if clients.insert(client.id(), client).is_some() {
            unreachable!("BUG: added two clients with the same ID");
        }
    }

    /// Get a single client.
    ///
    /// This returns owned data so the lock can be acquired and dropped
    /// immediately.
    pub async fn get_client(&self, id: usize) -> Option<Client> {
        let clients = self.clients.lock().await;
        clients.get(&id).map(|cd| cd.client.clone())
    }

    /// Return a snapshot of clients.
    ///
    /// This returns owned data so the lock can be acquired and dropped
    /// immediately.
    pub async fn clients(&self) -> Vec<Client> {
        let clients = self.clients.lock().await;
        clients.values().map(|cd| cd.client.clone()).collect()
    }

    /// Return one arbitrary client.
    ///
    /// This returns owned data so the lock can be acquired and dropped
    /// immediately.
    pub async fn one_client(&self) -> Option<Client> {
        let clients = self.clients.lock().await;
        clients.values().next().map(|cd| cd.client.clone())
    }

    /// Send cleanup messages and remove remove client for client map
    pub async fn cleanup_client(&self, client: Client) -> Result<()> {
        debug!("cleaning up client");

        let mut clients = self.clients.lock().await;

        let Some(client) = clients.remove(&client.id()) else {
            // TODO This happens for example when the language server died while
            // client was still connected, and the client cleanup is attempted
            // with the instance being gone already. We should try notifying
            // these clients immediately and handling the cleanup separately.
            bail!("client was not connected");
        };

        let files = client.files.into_iter().collect::<Vec<_>>();
        self.close_all_files(&clients, files)
            .await
            .context("error closing files")?;

        Ok(())
    }

    /// Send a message to the language server channel
    pub async fn send_message(&self, message: Message) -> Result<(), SendError<Message>> {
        self.server.send(message).await
    }

    /// Save registered capabilities to allow later replaying them to new clients
    async fn register_capabilities(&self, params: Value) -> Result<()> {
        let params =
            serde_json::from_value::<lsp::RegistrationParams>(params).context("parsing params")?;

        let mut dyn_capabilities = self.dynamic_capabilities.lock().await;
        for reg in params.registrations {
            dyn_capabilities.insert(reg.id.clone(), reg);
        }

        Ok(())
    }

    /// Cache a stateful notification for replay to new clients
    async fn cache_notification(&self, notif: Notification) {
        self.cached_notifications
            .lock()
            .await
            .insert(notif.method.clone(), notif);
    }

    /// Remove cached capability registration to stop replaying them to new clients
    async fn unregister_capabilities(&self, params: Value) -> Result<()> {
        let params = serde_json::from_value::<lsp::UnregistrationParams>(params)
            .context("parsing params")?;

        let mut dyn_capabilities = self.dynamic_capabilities.lock().await;
        for unreg in params.unregistrations {
            dyn_capabilities.remove(&unreg.id);
        }

        Ok(())
    }

    /// Handle `textDocument/didOpen` client notification
    pub async fn open_file(&self, client_id: usize, params: Value) -> Result<()> {
        let params = serde_json::from_value::<lsp::DidOpenTextDocumentParams>(params)
            .context("parsing params")?;
        let uri = &params.text_document.uri;

        let mut send_notification = true;

        let mut clients = self.clients.lock().await;
        for client in clients.values() {
            if client.files.contains(uri) {
                debug!(?uri, "file is already opened by another client");
                send_notification = false;
                break;
            }
        }

        clients
            .get_mut(&client_id)
            .context("no matching client")?
            .files
            .insert(uri.clone());

        if send_notification {
            let notif = Notification {
                jsonrpc: Version,
                method: "textDocument/didOpen".into(),
                params: serde_json::to_value(params).unwrap(),
            };
            debug!(?notif, "first client opened file");
            let _ = self.send_message(notif.into()).await;
        }

        Ok(())
    }

    /// Handle `textDocument/didClose` client notification
    pub async fn close_file(&self, client_id: usize, params: Value) -> Result<()> {
        let params = serde_json::from_value::<lsp::DidCloseTextDocumentParams>(params)
            .context("parsing params")?;

        let mut clients = self.clients.lock().await;

        clients
            .get_mut(&client_id)
            .context("no matching client")?
            .files
            .remove(&params.text_document.uri);

        self.close_all_files(&clients, vec![params.text_document.uri])
            .await
    }

    /// Handle closing many files at once and sending notifications for
    /// definitely closed files
    async fn close_all_files(
        &self,
        clients: &HashMap<usize, ClientData>,
        files: Vec<String>,
    ) -> Result<()> {
        for uri in files {
            let mut send_notification = true;

            for client in clients.values() {
                if client.files.contains(&uri) {
                    debug!(?uri, "file still opened by another client");
                    send_notification = false;
                    break;
                }
            }

            if send_notification {
                let params = lsp::DidCloseTextDocumentParams {
                    text_document: lsp::TextDocumentIdentifier { uri },
                };
                let notif = Notification {
                    jsonrpc: Version,
                    method: "textDocument/didClose".into(),
                    params: serde_json::to_value(params).unwrap(),
                };
                debug!(?notif, "last client closed file");
                let _ = self.send_message(notif.into()).await;
            }
        }

        Ok(())
    }

    pub async fn get_status(&self) -> ext::Instance {
        let clients = self
            .clients
            .lock()
            .await
            .values()
            .map(|client| client.get_status())
            .collect();

        let registered_dyn_capabilities = self
            .dynamic_capabilities
            .lock()
            .await
            .values()
            .map(|reg| reg.method.clone())
            .collect();

        ext::Instance {
            pid: self.pid,
            server: self.key.server.clone(),
            args: self.key.args.clone(),
            env: self.key.env.clone(),
            match_env: self.key.match_env().clone(),
            workspace_root: ext::WorkspaceRoot {
                path: self.key.workspace_root.path.clone(),
                device_id: self.key.workspace_root.device_id,
                file_id: self.key.workspace_root.file_id,
            },
            idle_for: self.idle(),
            clients,
            registered_dyn_capabilities,
        }
    }

    /// Read all files currently open by any client from disk and generate a
    /// `textDocument/didChange` notification with the full content for each
    /// of them.
    pub async fn sync_files_from_disk(&self) -> Result<()> {
        let unique_files = self
            .clients
            .lock()
            .await
            .values()
            .flat_map(|client| client.files.iter().cloned())
            .collect::<HashSet<String>>();

        for uri in unique_files {
            let path = match lsp::parse_file_uri(&uri) {
                Ok(path) => path,
                Err(err) => {
                    warn!(?uri, ?err, "failed to parse URI");
                    continue;
                }
            };

            let text = match tokio::fs::read_to_string(&path).await {
                Ok(text) => text,
                Err(err) => {
                    warn!(?path, ?err, "failed to read file from disk");
                    continue;
                }
            };

            let params = lsp::DidChangeTextDocumentParams {
                text_document: lsp::VersionedTextDocumentIdentifier { uri, version: None },
                content_changes: vec![lsp::TextDocumentContentChangeEvent { text }],
            };

            let notif = Notification {
                jsonrpc: Version,
                method: "textDocument/didChange".into(),
                params: serde_json::to_value(params).unwrap(),
            };

            self.send_message(notif.into())
                .await
                .context("instance closed")?;
        }

        Ok(())
    }
}

pub struct InstanceMap {
    instances: HashMap<InstanceKey, Arc<Instance>>,
    match_environment: Arc<Vec<Filter>>,
}

impl InstanceMap {
    pub fn new(config: &Config) -> Arc<Mutex<Self>> {
        let instance_map = Arc::new(Mutex::new(InstanceMap {
            instances: HashMap::new(),
            match_environment: Arc::new(config.match_environment.clone()),
        }));
        task::spawn(gc_task(
            instance_map.clone(),
            config.gc_interval,
            config.instance_timeout,
        ));
        instance_map
    }

    pub fn match_environment(&self) -> Arc<Vec<Filter>> {
        self.match_environment.clone()
    }

    /// Finds an instance with the longest path such as
    /// `cwd.starts_with(workspace_root)` is true.
    ///
    /// This returns owned data to allow callers to drop the lock immediately.
    pub fn get_by_cwd(&self, cwd: &str) -> Option<Arc<Instance>> {
        self.instances
            .iter()
            .filter(|(key, _)| Path::new(cwd).starts_with(&key.workspace_root.path))
            .max_by_key(|(key, _)| key.workspace_root.path.len())
            .map(|(_, inst)| inst.clone())
    }

    /// Return a snapshot of instances.
    ///
    /// This returns owned data to allow callers to drop the lock immediately.
    pub fn instances(&self) -> Vec<Arc<Instance>> {
        self.instances.values().cloned().collect()
    }
}

/// Periodically check for for idle language server instances
#[instrument("garbage collector", skip_all)]
async fn gc_task(
    instance_map: Arc<Mutex<InstanceMap>>,
    gc_interval: u32,
    instance_timeout: Option<u32>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(gc_interval.into()));
    loop {
        interval.tick().await;

        let instances = instance_map.lock().await.instances();
        for instance in &instances {
            // The instance might have been removed from the map since we took
            // the snapshot. If so, wait_task has already exited and
            // notify_one below is a no-op. Otherwise, wait_task will receive
            // the notification and kill the child.
            let clients = instance.clients.lock().await;

            let idle = instance.idle();
            debug!(path = ?instance.key.workspace_root, idle, clients = clients.len(), "check instance");

            if let Some(instance_timeout) = instance_timeout {
                // Close timed out instance
                if idle > i64::from(instance_timeout) && clients.is_empty() {
                    info!(pid = instance.pid, path = ?instance.key.workspace_root, idle, "instance timed out");
                    instance.close.notify_one();
                }
            }
        }
    }
}

/// Find existing or spawn a new language server instance
///
/// The instance is looked up based on `instance_key`. If an existing one is
/// found then it's returned and `init_req_params` are discarded. If it's
/// not found a new instance is spawned and initialized using the provided
/// `init_req_params`, this instance is then inserted into the map and returned.
pub async fn get_or_spawn(
    map: Arc<Mutex<InstanceMap>>,
    key: InstanceKey,
    init_req_params: lsp::InitializeParams,
    client: Client,
) -> Result<Arc<Instance>> {
    // Fast path: reuse an existing instance without blocking on spawn.
    {
        let map_guard = map.lock().await;
        if let Some(instance) = map_guard.instances.get(&key) {
            info!("reusing language server instance");
            let instance = instance.clone();
            drop(map_guard);
            instance.add_client(client).await;
            return Ok(instance);
        }
    }

    // Slow path: spawn a new instance without holding the map lock.
    // Clone the client so we can re-register it if we lose a spawn race.
    let instance = spawn(key.clone(), init_req_params, map.clone(), client.clone())
        .await
        .context("spawning instance")?;

    // Re-acquire the lock and insert, handling the rare case where another
    // task spawned an instance for the same key while we were blocked.
    let mut map_guard = map.lock().await;
    match map_guard.instances.entry(key) {
        Entry::Occupied(e) => {
            let winner = e.get().clone();
            drop(map_guard);
            instance.close.notify_one();
            winner.add_client(client).await;
            Ok(winner)
        }
        Entry::Vacant(e) => {
            e.insert(instance.clone());
            Ok(instance)
        }
    }
}

#[instrument(name = "instance", fields(pid = field::Empty), skip_all, parent = None)]
async fn spawn(
    key: InstanceKey,
    mut init_req_params: lsp::InitializeParams,
    // NOTE: The caller may not be holding the instance map lock. Spawned
    // tasks (e.g. wait_task) may lock the map independently.
    map: Arc<Mutex<InstanceMap>>,
    client: Client,
) -> Result<Arc<Instance>> {
    let mut child = Command::new(&key.server)
        .args(&key.args)
        .envs(&key.env)
        .current_dir(&key.workspace_root.path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| {
            let InstanceKey {
                server,
                args,
                env,
                workspace_root,
                ..
            } = &key;
            let path = env
                .get("PATH")
                .map(<_>::to_owned)
                // Display PATH from our environment Command will if none was
                // passed from the client environment.
                .or_else(|| env::var("PATH").ok())
                .unwrap_or_default();
            format!(
                "spawning language server: server={server:?}, args={args:?}, \
                cwd={workspace_root:?}, path={path:?}, env={env:?}",
            )
        })?;

    let pid = child.id().context("child exited early, couldn't get PID")?;
    tracing::Span::current().record("pid", pid);

    info!(server = ?key.server, args = ?key.args, cwd = ?key.workspace_root, "spawned language server");

    let stderr = child.stderr.take().unwrap();
    task::spawn(stderr_task(stderr).in_current_span());

    let stdout = child.stdout.take().unwrap();
    let mut reader = LspReader::new(BufReader::new(stdout), "server");

    let stdin = child.stdin.take().unwrap();
    let mut writer = LspWriter::new(stdin, "server");

    // Some LSP servers monitor client PIDs and exit if none remain (as
    // documented in the LSP spec). We need to replace the client PID in the
    // initialization request with lspmux server PID to prevent the server from
    // exitting when the original client exits.
    init_req_params.process_id = Some(std::process::id() as u64);

    let init_result = initialize_handshake(init_req_params, &mut reader, &mut writer)
        .await
        .context("server handshake")?;

    info!("initialized server");

    let (message_writer, rx) = mpsc::channel(64);

    let mut clients = HashMap::new();
    let client = ClientData {
        client,
        files: HashSet::new(),
    };
    clients.insert(client.id(), client);

    let instance = Arc::new(Instance {
        key,
        pid,
        init_result,
        server: message_writer,
        clients: Mutex::new(clients),
        dynamic_capabilities: Mutex::default(),
        cached_notifications: Mutex::default(),
        close: Notify::new(),
        last_used: AtomicI64::new(elapsed_seconds()),
    });

    task::spawn(stdout_task(instance.clone(), reader).in_current_span());
    task::spawn(stdin_task(rx, writer).in_current_span());

    task::spawn(wait_task(instance.clone(), map, child).in_current_span());

    Ok(instance)
}

#[cfg(test)]
mod tests {
    use super::{InstanceKey, WorkspaceRoot};
    use crate::config::Filter;
    use glob::Pattern;
    use std::collections::BTreeMap;

    fn workspace_root() -> WorkspaceRoot {
        WorkspaceRoot {
            path: "/tmp/workspace".into(),
            device_id: 1,
            file_id: 2,
        }
    }

    #[test]
    fn instance_key_ignores_all_env_with_empty_match_filters() {
        let env_a = BTreeMap::from([
            ("PATH".to_string(), "/bin".to_string()),
            ("OPENAI_API_KEY".to_string(), "key-a".to_string()),
            ("RUSTUP_TOOLCHAIN".to_string(), "nightly".to_string()),
        ]);
        let env_b = BTreeMap::from([
            ("PATH".to_string(), "/usr/bin".to_string()),
            ("OPENAI_API_KEY".to_string(), "key-b".to_string()),
            ("RUSTUP_TOOLCHAIN".to_string(), "stable".to_string()),
        ]);

        let filters: Vec<Filter> = vec![];
        let key_a =
            InstanceKey::new("rust-analyzer".into(), vec![], env_a, workspace_root(), &filters);
        let key_b =
            InstanceKey::new("rust-analyzer".into(), vec![], env_b, workspace_root(), &filters);

        assert_eq!(key_a, key_b);
    }

    #[test]
    fn instance_key_matches_only_specified_env_vars() {
        let env_a = BTreeMap::from([
            ("RUSTUP_TOOLCHAIN".to_string(), "nightly".to_string()),
            ("PATH".to_string(), "/bin".to_string()),
            ("NVIM".to_string(), "1".to_string()),
        ]);
        let env_b = BTreeMap::from([
            ("RUSTUP_TOOLCHAIN".to_string(), "nightly".to_string()),
            ("PATH".to_string(), "/usr/bin".to_string()),
            ("NVIM".to_string(), "2".to_string()),
        ]);

        let filters = vec![Filter {
            negative: false,
            pattern: Pattern::new("RUSTUP_TOOLCHAIN").unwrap(),
        }];

        let key_a = InstanceKey::new(
            "rust-analyzer".into(),
            vec![],
            env_a.clone(),
            workspace_root(),
            &filters,
        );
        let key_b =
            InstanceKey::new("rust-analyzer".into(), vec![], env_b, workspace_root(), &filters);

        // PATH and NVIM differ but aren't in match_environment
        assert_eq!(key_a, key_b);

        // Different RUSTUP_TOOLCHAIN -> different keys
        let mut env_c = env_a;
        env_c.insert("RUSTUP_TOOLCHAIN".to_string(), "stable".to_string());
        let key_c =
            InstanceKey::new("rust-analyzer".into(), vec![], env_c, workspace_root(), &filters);
        assert_ne!(key_a, key_c);
    }

    #[test]
    fn instance_key_match_env_supports_negative_filters() {
        let env = BTreeMap::from([
            ("RUSTUP_TOOLCHAIN".to_string(), "nightly".to_string()),
            ("RUSTUP_HOME".to_string(), "/home/user/.rustup".to_string()),
            (
                "CARGO_TARGET_DIR".to_string(),
                "/tmp/target".to_string(),
            ),
        ]);

        let filters = vec![
            Filter {
                negative: false,
                pattern: Pattern::new("RUST*").unwrap(),
            },
            Filter {
                negative: true,
                pattern: Pattern::new("RUSTUP_HOME").unwrap(),
            },
        ];

        let key = InstanceKey::new(
            "rust-analyzer".into(),
            vec![],
            env,
            workspace_root(),
            &filters,
        );

        assert_eq!(
            key.match_env(),
            &BTreeMap::from([("RUSTUP_TOOLCHAIN".to_string(), "nightly".to_string()),])
        );
    }
}

#[instrument(skip_all)]
async fn initialize_handshake(
    init_req_params: lsp::InitializeParams,
    reader: &mut LspReader<BufReader<ChildStdout>>,
    writer: &mut LspWriter<ChildStdin>,
) -> Result<lsp::InitializeResult> {
    let request_id = "lspmux:initialize_request";

    // Use the first client's `InitializeParams` to initialize server. We assume
    // all subsequent clients configuration will be somewhat compatible with
    // whatever the first client negotiated for the same `workspace_root`,
    // `server` and `args`.
    let req = Request {
        jsonrpc: Version,
        method: "initialize".into(),
        params: serde_json::to_value(init_req_params).unwrap(),
        id: RequestId::String(request_id.into()),
    };
    writer
        .write_message(&req.into())
        .await
        .context("send initialize request")?;

    // TODO: Ignoring messages here is not ideal. The spec explicitly permits
    // sending `window/showMessage`, `window/logMessage`, `telemetry/event`,
    // `window/showMessageRequest` and when configured also `$/progress` [1],
    // which we should forward to the client(s). Not forwarding them means
    // the server will appear unresponsive while it's initializing.
    //
    // [1]: https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/#initialize
    //
    let res = loop {
        match reader
            .read_message()
            .await
            .context("receive initialize response")?
            .context("stream ended")?
        {
            Message::ResponseSuccess(res) if res.id == request_id => break res,
            msg => {
                warn!(
                    ?msg,
                    "ignoring message while waiting for initialize response"
                );
            }
        }
    };
    let result = serde_json::from_value(res.result).context("parse initialize response result")?;

    // Send a "fake" `initialized` notification to the server. We wait for them
    // from each client's `initialized` notification ourselves and they don't
    // contain any data that would need passing on.
    let init_notif = Notification {
        jsonrpc: Version,
        method: "initialized".into(),
        params: json!({}),
    };
    writer
        .write_message(&init_notif.into())
        .await
        .context("send initialized notification")?;

    Ok(result)
}

/// Read errors from language server stderr and log them
async fn stderr_task(stderr: ChildStderr) {
    let mut stderr = BufReader::new(stderr);
    let mut buffer = String::new();

    loop {
        buffer.clear();
        match stderr.read_line(&mut buffer).await {
            Ok(0) => {
                // reached EOF
                debug!("stderr closed");
                break;
            }
            Ok(_) => {
                let line = buffer.trim_end(); // remove trailing '\n' or possibly '\r\n'
                error!(%line, "stderr");
            }
            Err(err) => {
                let err = anyhow::Error::from(err);
                error!(?err, "error reading from stderr");
            }
        }
    }
}

/// Receive messages from clients' channel and write them into language server stdin
async fn stdin_task(mut receiver: mpsc::Receiver<Message>, mut writer: LspWriter<ChildStdin>) {
    // Because we (stdin task) don't keep a reference to `self` it will be dropped when the
    // child closes and all the clients disconnect including the sender and this receiver
    // will not keep blocking (unlike in client input task)
    while let Some(message) = receiver.recv().await {
        if let Err(err) = writer.write_message(&message).await {
            match err.kind() {
                // stdin is closed, no need to log an error
                ErrorKind::BrokenPipe => {}
                _ => {
                    let err = anyhow::Error::from(err);
                    error!(?err, "error writing to stdin");
                }
            }
            break;
        }
    }
    debug!("stdin closed");
}

/// Wait for child and log when it exits
async fn wait_task(
    instance: Arc<Instance>,
    instance_map: Arc<Mutex<InstanceMap>>,
    mut child: Child,
) {
    let key = instance.key.clone();
    loop {
        select! {
            _ = instance.close.notified() => {
                if let Err(err) = child.start_kill() {
                    error!(?err, "failed to close child");
                }
            }
            exit = child.wait() => {
                // Remove ourselves from the map so new clients spawn their own
                // instance. Guard with ptr_eq to avoid removing a different
                // instance that may have replaced us after a spawn race.
                {
                    let mut map = instance_map.lock().await;
                    if map.instances.get(&key).is_some_and(|i| Arc::ptr_eq(i, &instance)) {
                        map.instances.remove(&key);
                    }
                }

                // Disconnect all current clients
                //
                // We'll rely on the editor client to restart the lspmux client,
                // start a new connection and we'll spawn another instance like
                // we'd with any other new client.
                instance.clients.lock().await.clear();

                match exit {
                    Ok(status) => {
                        #[cfg(unix)]
                        let signal = std::os::unix::process::ExitStatusExt::signal(&status);
                        #[cfg(not(unix))]
                        let signal = tracing::field::Empty;

                        error!(
                            success = status.success(),
                            code = status.code(),
                            signal,
                            "child exited",
                        );
                    }
                    Err(err) => error!(?err, "error waiting for child"),
                }
                break;
            }
        }
    }
}

/// Read messages from server stdout and send them to corresponding client channels
async fn stdout_task(instance: Arc<Instance>, mut reader: LspReader<BufReader<ChildStdout>>) {
    loop {
        let message = match reader.read_message().await {
            Ok(Some(message)) => message,
            Ok(None) => {
                debug!("stdout closed");
                break;
            }
            Err(err) => {
                error!(?err, "reading message");
                continue;
            }
        };

        // Each match arm holds a lock for as little as possible (this is
        // encapsulated within `get_client`, etc). The lock protocol prevents a
        // slow client from holding the lock and blocking all other clients and
        // lock-dependent operations.
        match message {
            Message::ResponseSuccess(mut res) => {
                // Forward successful response to the right client based on the
                // Request ID tag.
                match res.id.untag() {
                    (Some(Tag::ClientId(client_id)), id) => {
                        res.id = id;
                        if let Some(client) = instance.get_client(client_id).await {
                            let _ = client.send_message(res.into()).await;
                        } else {
                            debug!(?client_id, "no matching client");
                        }
                    }
                    (Some(Tag::Drop), _) => {
                        // Drop the message
                    }
                    _ => {
                        warn!(?res, "ignoring improperly tagged server response")
                    }
                }
            }

            Message::ResponseError(mut res) => {
                // Forward the error response to the right client based on the
                // Request ID tag.
                match res.id.untag() {
                    (Some(Tag::ClientId(client_id)), id) => {
                        warn!(?res, "server responded with error");
                        res.id = id;
                        if let Some(client) = instance.get_client(client_id).await {
                            let _ = client.send_message(res.into()).await;
                        } else {
                            debug!(?client_id, "no matching client");
                        }
                    }
                    (Some(Tag::Drop), _) => {
                        // Drop the message
                    }
                    _ => {
                        warn!(?res, "ignoring improperly tagged server response")
                    }
                }
            }

            Message::Request(mut req)
                if [
                    "window/workDoneProgress/create",
                    "workspace/codeLens/refresh",
                    "workspace/semanticTokens/refresh",
                    "workspace/inlayHint/refresh",
                    "workspace/inlineValue/refresh",
                    "workspace/diagnostic/refresh",
                ]
                .contains(&req.method.as_str()) =>
            {
                // All these server requests have null responses and we need
                // to inform all clients. We can forward the request to all
                // clients, send a fake successful response and ignore the real
                // client responses.
                trace!(?req, "server request {}", req.method.as_str());

                let id = req.id;
                req.id = id.tag(Tag::Drop);

                for client in instance.clients().await {
                    let _ = client.send_message(req.clone().into()).await;
                }

                let _ = instance
                    .send_message(ResponseSuccess::null(id).into())
                    .await;
            }

            Message::Request(mut req) if req.method == "workspace/configuration" => {
                // Response to `workspace/configuration` should be the same from
                // any client. So we'll just pick the first and let it answer.
                debug!(?req, "server request workspace/configuration");

                req.id = req.id.tag(Tag::Forward);

                if let Some(client) = instance.one_client().await {
                    let _ = client.send_message(req.into()).await;
                } else {
                    // If there is no client connected at this moment we'll
                    // ignore the request.
                }
            }

            Message::Request(mut req) if req.method == "client/registerCapability" => {
                // These need to be forwarded to every client so they're aware
                // of the capability. The response doesn't contain anything
                // important so we can safely ignore the real answers and send a
                // fake one to the server.
                debug!(?req, "server request client/registerCapability");

                let id = req.id;
                req.id = id.tag(Tag::Drop);

                // Cache before broadcasting so that a client connecting
                // between the cache update and the broadcast will see
                // the capability via add_client's replay. The worst case
                // is a duplicate registerCapability, which is benign.
                if let Err(err) = instance.register_capabilities(req.params.clone()).await {
                    warn!(?err, "error registering capabilities");
                }

                for client in instance.clients().await {
                    let _ = client.send_message(req.clone().into()).await;
                }

                let _ = instance
                    .send_message(ResponseSuccess::null(id).into())
                    .await;
            }

            Message::Request(mut req) if req.method == "client/unregisterCapability" => {
                // These need to be forwarded to every client so they're aware
                // of the capability not being available anymore. The response
                // doesn't contain anything important so we can safely ignore
                // the real answers and send a fake one to the server.
                debug!(?req, "server request client/unregisterCapability");

                let id = req.id;
                req.id = id.tag(Tag::Drop);

                // Uncache before broadcasting so that a client connecting
                // between the cache update and the broadcast won't receive
                // the stale capability via add_client's replay.
                if let Err(err) = instance.unregister_capabilities(req.params.clone()).await {
                    warn!(?err, "error unregistering capabilities");
                }

                for client in instance.clients().await {
                    let _ = client.send_message(req.clone().into()).await;
                }

                let _ = instance
                    .send_message(ResponseSuccess::null(id).into())
                    .await;
            }

            Message::Request(req) => {
                // Unimplemented server -> client requests I've found in the LSP Spec.
                // TODO workspace/workspaceFolders request
                // TODO workspace/applyEdit request
                debug!(message = ?req, "ignoring unknown server request");
            }

            Message::Notification(notif) => {
                // Cache stateful notifications before broadcasting so that a
                // client connecting between the cache update and the broadcast
                // receives the state via add_client's replay (duplicate at
                // worst, which is benign).
                if is_stateful_notification(&notif.method) {
                    instance.cache_notification(notif.clone()).await;
                }

                // Server notifications don't expect a response. We can forward
                // them to all clients.
                for client in instance.clients().await {
                    let _ = client.send_message(notif.clone().into()).await;
                }
            }
        }
    }
}
