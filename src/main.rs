use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::env;
use std::error::Error;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;

const MANAGED_MONITOR: &str = "eDP-1";
const STATIC_FOOT_TAG: u32 = 1;
const STATIC_EMACS_TAG: u32 = 2;
const DYNAMIC_TAG_FIRST: u32 = 3;
const DYNAMIC_TAG_LAST: u32 = 10;
const CONTROL_SOCKET_NAME: &str = "mango-dynamic-tags.sock";
const STATE_FILE_NAME: &str = "mango-dynamic-tags-state.json";

type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[derive(Debug, Clone, Deserialize)]
struct Client {
    id: u32,

    #[serde(default)]
    appid: String,

    #[serde(default)]
    monitor: String,

    #[serde(default)]
    tags: Vec<u32>,

    #[serde(default)]
    is_focused: bool,
}

#[derive(Debug, Deserialize)]
struct ClientsSnapshot {
    clients: Vec<Client>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedState {
    mango_socket: String,
    manual_static_stack: Vec<u32>,
}

#[derive(Debug, Clone, Copy)]
enum ControlCommand {
    Next,
    Prev,
    To(u32),
}

#[derive(Debug)]
struct ControlRequest {
    command: ControlCommand,
    reply: Sender<std::result::Result<String, String>>,
}

#[derive(Debug)]
enum Event {
    ClientsChanged,
    Focus(Client),
    Control(ControlRequest),
    WatcherError(&'static str, String),
    WatcherEnded(&'static str),
}

struct DynamicTags {
    clients: HashMap<u32, Client>,

    // Clients owning one of tags 3-10.
    managed: HashSet<u32>,

    // Clients created after tags 3-10 were full. They remain on the
    // tag where Mango opened them and do not participate in dynamic MRU.
    overflow: HashSet<u32>,

    // Non-Foot/Emacs clients explicitly assigned to tag 1 or 2.
    // The daemon leaves these stacked there until the user explicitly
    // moves them again.
    manual_static_stack: HashSet<u32>,

    // Clients waiting for enough metadata to classify.
    pending: HashSet<u32>,

    // Front = most recently focused managed dynamic client.
    mru: VecDeque<u32>,

    focused_id: Option<u32>,

    state_path: PathBuf,
    mango_socket: String,
}

fn static_tag(appid: &str) -> Option<u32> {
    match appid {
        "foot" => Some(STATIC_FOOT_TAG),
        "emacs" => Some(STATIC_EMACS_TAG),
        _ => None,
    }
}

fn on_managed_monitor(client: &Client) -> bool {
    client.monitor == MANAGED_MONITOR
}

fn single_tag(client: &Client) -> Option<u32> {
    if client.tags.len() == 1 {
        Some(client.tags[0])
    } else {
        None
    }
}

fn single_dynamic_tag(client: &Client) -> Option<u32> {
    let tag = single_tag(client)?;

    if (DYNAMIC_TAG_FIRST..=DYNAMIC_TAG_LAST).contains(&tag) {
        Some(tag)
    } else {
        None
    }
}

fn index_clients(clients: Vec<Client>) -> HashMap<u32, Client> {
    clients
        .into_iter()
        .map(|client| (client.id, client))
        .collect()
}

fn runtime_dir() -> Result<PathBuf> {
    if let Some(dir) = env::var_os("XDG_RUNTIME_DIR") {
        return Ok(PathBuf::from(dir));
    }

    Err("XDG_RUNTIME_DIR is not set".into())
}

fn control_socket_path() -> Result<PathBuf> {
    Ok(runtime_dir()?.join(CONTROL_SOCKET_NAME))
}

fn state_file_path() -> Result<PathBuf> {
    Ok(runtime_dir()?.join(STATE_FILE_NAME))
}

fn ipc_request(socket_path: &str, command: &str) -> Result<String> {
    let mut stream = UnixStream::connect(socket_path)?;

    stream.write_all(command.as_bytes())?;
    stream.write_all(b"\n")?;
    stream.flush()?;

    let mut response = String::new();
    BufReader::new(stream).read_to_string(&mut response)?;

    Ok(response)
}

fn dispatch(socket_path: &str, action: &str, client_id: Option<u32>) -> Result<()> {
    let command = match client_id {
        Some(id) => format!("dispatch {action} client,{id}"),
        None => format!("dispatch {action}"),
    };

    let response = ipc_request(socket_path, &command)?;

    if let Ok(value) = serde_json::from_str::<Value>(&response) {
        if value.get("success").and_then(Value::as_bool) == Some(false) {
            return Err(format!(
                "Mango rejected IPC command `{command}`: {}",
                response.trim()
            )
            .into());
        }
    }

    Ok(())
}

fn move_silent(socket_path: &str, client_id: u32, tag: u32) -> Result<()> {
    dispatch(socket_path, &format!("tagsilent,{tag}"), Some(client_id))
}

fn focus_client(socket_path: &str, client_id: u32) -> Result<()> {
    dispatch(socket_path, "focusid", Some(client_id))
}

fn view_tag(socket_path: &str, tag: u32) -> Result<()> {
    dispatch(socket_path, &format!("view,{tag}"), None)
}

fn view_and_focus_client(socket_path: &str, client_id: u32, tag: u32) -> Result<()> {
    view_tag(socket_path, tag)?;
    focus_client(socket_path, client_id)?;
    Ok(())
}

fn get_clients(socket_path: &str) -> Result<Vec<Client>> {
    let response = ipc_request(socket_path, "get all-clients")?;
    let snapshot: ClientsSnapshot = serde_json::from_str(&response)?;
    Ok(snapshot.clients)
}

fn validate_tag_capacity(socket_path: &str) -> Result<()> {
    let response = ipc_request(socket_path, "get all-tags")?;
    let value: Value = serde_json::from_str(&response)?;

    let max_tag = value
        .get("all_tags")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|monitor| monitor.get("tags")?.as_array())
        .flatten()
        .filter_map(|tag| tag.get("index")?.as_u64())
        .max()
        .unwrap_or(0);

    if max_tag < u64::from(DYNAMIC_TAG_LAST) {
        return Err(format!(
            "Mango only exposes tags through {max_tag}; this helper requires tag_num={DYNAMIC_TAG_LAST}"
        )
        .into());
    }

    Ok(())
}

impl DynamicTags {
    fn new(mango_socket: String, state_path: PathBuf) -> Self {
        Self {
            clients: HashMap::new(),
            managed: HashSet::new(),
            overflow: HashSet::new(),
            manual_static_stack: HashSet::new(),
            pending: HashSet::new(),
            mru: VecDeque::new(),
            focused_id: None,
            state_path,
            mango_socket,
        }
    }

    fn load_persisted_state(&mut self) {
        let Ok(contents) = fs::read_to_string(&self.state_path) else {
            return;
        };

        let Ok(state) = serde_json::from_str::<PersistedState>(&contents) else {
            return;
        };

        if state.mango_socket != self.mango_socket {
            return;
        }

        self.manual_static_stack = state.manual_static_stack.into_iter().collect();
    }

    fn persist_state(&self) -> Result<()> {
        let mut manual_static_stack: Vec<u32> = self.manual_static_stack.iter().copied().collect();
        manual_static_stack.sort_unstable();

        let state = PersistedState {
            mango_socket: self.mango_socket.clone(),
            manual_static_stack,
        };

        let serialized = serde_json::to_string(&state)?;
        fs::write(&self.state_path, serialized)?;
        Ok(())
    }

    fn touch_mru(&mut self, client_id: u32) {
        if let Some(position) = self.mru.iter().position(|id| *id == client_id) {
            self.mru.remove(position);
        }

        self.mru.push_front(client_id);
    }

    fn remove_mru(&mut self, client_id: u32) {
        if let Some(position) = self.mru.iter().position(|id| *id == client_id) {
            self.mru.remove(position);
        }
    }

    fn first_free_tag(&self, ignore_id: Option<u32>) -> Option<u32> {
        let mut occupied = HashSet::new();

        for client in self.clients.values() {
            if Some(client.id) == ignore_id || !on_managed_monitor(client) {
                continue;
            }

            for tag in &client.tags {
                if (DYNAMIC_TAG_FIRST..=DYNAMIC_TAG_LAST).contains(tag) {
                    occupied.insert(*tag);
                }
            }
        }

        (DYNAMIC_TAG_FIRST..=DYNAMIC_TAG_LAST).find(|tag| !occupied.contains(tag))
    }

    fn enforce_static_clients(&mut self, socket_path: &str) -> Result<()> {
        let static_clients: Vec<(u32, u32)> = self
            .clients
            .values()
            .filter(|client| on_managed_monitor(client))
            .filter_map(|client| static_tag(&client.appid).map(|tag| (client.id, tag)))
            .collect();

        for (client_id, target_tag) in static_clients {
            self.managed.remove(&client_id);
            self.overflow.remove(&client_id);
            self.manual_static_stack.remove(&client_id);
            self.remove_mru(client_id);

            let needs_move = self
                .clients
                .get(&client_id)
                .is_some_and(|client| client.tags.len() != 1 || client.tags[0] != target_tag);

            if needs_move {
                eprintln!("mango-dynamic-tags: static client {client_id} -> tag {target_tag}");

                move_silent(socket_path, client_id, target_tag)?;

                if let Some(client) = self.clients.get_mut(&client_id) {
                    client.tags = vec![target_tag];
                }
            }
        }

        Ok(())
    }

    fn initial_reconcile(&mut self, socket_path: &str) -> Result<()> {
        self.load_persisted_state();
        self.clients = index_clients(get_clients(socket_path)?);

        // Drop persisted IDs that no longer exist or are no longer valid manual stacks.
        self.manual_static_stack.retain(|client_id| {
            self.clients.get(client_id).is_some_and(|client| {
                on_managed_monitor(client)
                    && static_tag(&client.appid).is_none()
                    && matches!(
                        single_tag(client),
                        Some(STATIC_FOOT_TAG) | Some(STATIC_EMACS_TAG)
                    )
            })
        });

        self.enforce_static_clients(socket_path)?;
        self.clients = index_clients(get_clients(socket_path)?);

        self.managed.clear();
        self.overflow.clear();
        self.pending.clear();
        self.mru.clear();

        let mut used_tags = HashSet::new();
        let mut needs_tag = Vec::new();

        let mut dynamic_clients: Vec<Client> = self
            .clients
            .values()
            .filter(|client| {
                on_managed_monitor(client)
                    && static_tag(&client.appid).is_none()
                    && !self.manual_static_stack.contains(&client.id)
            })
            .cloned()
            .collect();

        dynamic_clients.sort_by_key(|client| client.id);

        // Preserve clients already occupying a unique dynamic tag.
        for client in dynamic_clients {
            match single_dynamic_tag(&client) {
                Some(tag) if used_tags.insert(tag) => {
                    self.managed.insert(client.id);
                }
                _ => needs_tag.push(client.id),
            }
        }

        // Re-home everything else into the first free dynamic tag.
        for client_id in needs_tag {
            let free_tag =
                (DYNAMIC_TAG_FIRST..=DYNAMIC_TAG_LAST).find(|tag| !used_tags.contains(tag));

            match free_tag {
                Some(tag) => {
                    eprintln!("mango-dynamic-tags: existing client {client_id} -> tag {tag}");

                    move_silent(socket_path, client_id, tag)?;

                    if let Some(client) = self.clients.get_mut(&client_id) {
                        client.tags = vec![tag];
                    }

                    used_tags.insert(tag);
                    self.managed.insert(client_id);
                }
                None => {
                    eprintln!(
                        "mango-dynamic-tags: pool full during startup; client {client_id} left in place"
                    );
                    self.overflow.insert(client_id);
                }
            }
        }

        self.clients = index_clients(get_clients(socket_path)?);

        let mut managed_by_tag: Vec<(u32, u32)> = self
            .managed
            .iter()
            .filter_map(|client_id| {
                let client = self.clients.get(client_id)?;
                let tag = single_dynamic_tag(client)?;
                Some((tag, *client_id))
            })
            .collect();

        managed_by_tag.sort_by_key(|(tag, _)| *tag);

        for (_, client_id) in managed_by_tag {
            self.mru.push_back(client_id);
        }

        if let Some(focused) = self
            .clients
            .values()
            .find(|client| client.is_focused)
            .cloned()
        {
            self.focused_id = Some(focused.id);

            if self.managed.contains(&focused.id) {
                self.touch_mru(focused.id);
            }
        }

        self.persist_state()?;
        Ok(())
    }

    fn process_pending(&mut self, socket_path: &str) -> Result<bool> {
        let mut ids: Vec<u32> = self.pending.iter().copied().collect();
        ids.sort_unstable();

        let mut focused_new_client = false;

        for client_id in ids {
            let Some(client) = self.clients.get(&client_id).cloned() else {
                self.pending.remove(&client_id);
                continue;
            };

            if !on_managed_monitor(&client) {
                self.pending.remove(&client_id);
                continue;
            }

            if client.appid.is_empty() {
                continue;
            }

            if let Some(tag) = static_tag(&client.appid) {
                if client.tags.len() != 1 || client.tags[0] != tag {
                    move_silent(socket_path, client_id, tag)?;

                    if let Some(current) = self.clients.get_mut(&client_id) {
                        current.tags = vec![tag];
                    }
                }

                self.managed.remove(&client_id);
                self.overflow.remove(&client_id);
                self.manual_static_stack.remove(&client_id);
                self.remove_mru(client_id);

                focus_client(socket_path, client_id)?;
                self.focused_id = Some(client_id);
                self.pending.remove(&client_id);
                focused_new_client = true;
                continue;
            }

            if self.manual_static_stack.contains(&client_id) {
                self.pending.remove(&client_id);
                continue;
            }

            match self.first_free_tag(Some(client_id)) {
                Some(tag) => {
                    eprintln!(
                        "mango-dynamic-tags: new client {client_id} ({}) -> tag {tag}",
                        client.appid
                    );

                    move_silent(socket_path, client_id, tag)?;

                    if let Some(current) = self.clients.get_mut(&client_id) {
                        current.tags = vec![tag];
                    }

                    self.overflow.remove(&client_id);
                    self.managed.insert(client_id);
                    self.touch_mru(client_id);
                    focus_client(socket_path, client_id)?;
                    self.focused_id = Some(client_id);
                }
                None => {
                    eprintln!(
                        "mango-dynamic-tags: pool full; leaving client {client_id} ({}) on its current tag",
                        client.appid
                    );

                    self.managed.remove(&client_id);
                    self.remove_mru(client_id);
                    self.overflow.insert(client_id);
                    focus_client(socket_path, client_id)?;
                    self.focused_id = Some(client_id);
                }
            }

            self.pending.remove(&client_id);
            focused_new_client = true;
        }

        Ok(focused_new_client)
    }

    fn focus_mru_or_terminal(&mut self, socket_path: &str) -> Result<()> {
        while let Some(client_id) = self.mru.front().copied() {
            let tag = self
                .clients
                .get(&client_id)
                .filter(|client| self.managed.contains(&client_id) && on_managed_monitor(client))
                .and_then(single_dynamic_tag);

            if let Some(tag) = tag {
                eprintln!("mango-dynamic-tags: returning to MRU client {client_id} on tag {tag}");
                view_and_focus_client(socket_path, client_id, tag)?;
                self.focused_id = Some(client_id);
                return Ok(());
            }

            self.mru.pop_front();
        }

        let mut survivors: Vec<(u32, u32)> = self
            .managed
            .iter()
            .filter_map(|client_id| {
                let client = self.clients.get(client_id)?;
                let tag = single_dynamic_tag(client)?;
                Some((tag, *client_id))
            })
            .collect();

        survivors.sort_by_key(|(tag, _)| *tag);

        if let Some((tag, client_id)) = survivors.first().copied() {
            self.touch_mru(client_id);
            view_and_focus_client(socket_path, client_id, tag)?;
            self.focused_id = Some(client_id);
            return Ok(());
        }

        eprintln!("mango-dynamic-tags: no managed dynamic clients; returning to tag 1");
        view_tag(socket_path, STATIC_FOOT_TAG)?;
        self.focused_id = None;
        Ok(())
    }

    fn handle_focus(&mut self, client: Client) {
        self.focused_id = Some(client.id);
        self.clients.insert(client.id, client.clone());

        if self.managed.contains(&client.id) {
            self.touch_mru(client.id);
        }
    }

    fn handle_clients(&mut self, socket_path: &str, clients: Vec<Client>) -> Result<()> {
        let old_clients = std::mem::take(&mut self.clients);
        let new_clients = index_clients(clients);

        let old_ids: HashSet<u32> = old_clients.keys().copied().collect();
        let new_ids: HashSet<u32> = new_clients.keys().copied().collect();

        let added_ids: HashSet<u32> = new_ids.difference(&old_ids).copied().collect();
        let removed_ids: Vec<u32> = old_ids.difference(&new_ids).copied().collect();

        let mut closed_focused_managed = false;
        let mut persisted_changed = false;

        for client_id in removed_ids {
            if let Some(old_client) = old_clients.get(&client_id) {
                let was_managed = self.managed.remove(&client_id);
                let was_focused = self.focused_id == Some(client_id) || old_client.is_focused;

                if was_managed && was_focused {
                    closed_focused_managed = true;
                }
            }

            self.overflow.remove(&client_id);
            self.pending.remove(&client_id);
            if self.manual_static_stack.remove(&client_id) {
                persisted_changed = true;
            }
            self.remove_mru(client_id);

            if self.focused_id == Some(client_id) {
                self.focused_id = None;
            }
        }

        self.clients = new_clients;

        // Static applications remain pinned to tags 1 and 2.
        self.enforce_static_clients(socket_path)?;

        // A managed client remains managed if it still occupies exactly one
        // dynamic tag. Moving between dynamic tags is valid and is how manual
        // swap operations work.
        let invalid_managed: Vec<u32> = self
            .managed
            .iter()
            .copied()
            .filter(|client_id| {
                self.clients.get(client_id).is_none_or(|client| {
                    !on_managed_monitor(client)
                        || static_tag(&client.appid).is_some()
                        || single_dynamic_tag(client).is_none()
                })
            })
            .collect();

        for client_id in invalid_managed {
            self.managed.remove(&client_id);
            self.remove_mru(client_id);

            let should_requeue = self.clients.get(&client_id).is_some_and(|client| {
                on_managed_monitor(client)
                    && static_tag(&client.appid).is_none()
                    && !self.manual_static_stack.contains(&client_id)
            });

            if should_requeue {
                self.pending.insert(client_id);
            }
        }

        let invalid_overflow: Vec<u32> = self
            .overflow
            .iter()
            .copied()
            .filter(|client_id| {
                self.clients.get(client_id).is_none_or(|client| {
                    !on_managed_monitor(client) || static_tag(&client.appid).is_some()
                })
            })
            .collect();

        for client_id in invalid_overflow {
            self.overflow.remove(&client_id);
        }

        for client_id in added_ids {
            self.pending.insert(client_id);
        }

        let unclassified: Vec<u32> = self
            .clients
            .values()
            .filter(|client| {
                on_managed_monitor(client)
                    && static_tag(&client.appid).is_none()
                    && !self.managed.contains(&client.id)
                    && !self.overflow.contains(&client.id)
                    && !self.manual_static_stack.contains(&client.id)
            })
            .map(|client| client.id)
            .collect();

        for client_id in unclassified {
            self.pending.insert(client_id);
        }

        let focused_new_client = self.process_pending(socket_path)?;

        if closed_focused_managed && !focused_new_client {
            self.focus_mru_or_terminal(socket_path)?;
        }

        if persisted_changed {
            self.persist_state()?;
        }

        Ok(())
    }

    fn refresh_snapshot(&mut self, socket_path: &str) -> Result<()> {
        self.clients = index_clients(get_clients(socket_path)?);

        let existing_ids: HashSet<u32> = self.clients.keys().copied().collect();
        self.managed.retain(|id| existing_ids.contains(id));
        self.overflow.retain(|id| existing_ids.contains(id));
        self.manual_static_stack
            .retain(|id| existing_ids.contains(id));
        self.pending.retain(|id| existing_ids.contains(id));
        self.mru.retain(|id| existing_ids.contains(id));

        if self
            .focused_id
            .is_some_and(|id| !existing_ids.contains(&id))
        {
            self.focused_id = None;
        }

        Ok(())
    }

    fn focused_client(&self) -> Result<Client> {
        self.clients
            .values()
            .find(|client| client.is_focused)
            .cloned()
            .or_else(|| {
                self.focused_id
                    .and_then(|client_id| self.clients.get(&client_id).cloned())
            })
            .ok_or_else(|| "no focused client".into())
    }

    fn clients_on_tag(&self, tag: u32, exclude_id: u32) -> Vec<Client> {
        let mut clients: Vec<Client> = self
            .clients
            .values()
            .filter(|client| {
                client.id != exclude_id && on_managed_monitor(client) && client.tags.contains(&tag)
            })
            .cloned()
            .collect();

        clients.sort_by_key(|client| client.id);
        clients
    }

    fn move_focused_to_static_tag(
        &mut self,
        socket_path: &str,
        client: &Client,
        target_tag: u32,
    ) -> Result<String> {
        if let Some(pinned_tag) = static_tag(&client.appid) {
            if target_tag != pinned_tag {
                return Err(format!("{} is pinned to tag {pinned_tag}", client.appid).into());
            }
        }

        move_silent(socket_path, client.id, target_tag)?;

        if let Some(current) = self.clients.get_mut(&client.id) {
            current.tags = vec![target_tag];
        }

        if static_tag(&client.appid).is_none() {
            self.managed.remove(&client.id);
            self.overflow.remove(&client.id);
            self.pending.remove(&client.id);
            self.remove_mru(client.id);
            self.manual_static_stack.insert(client.id);
            self.persist_state()?;
        }

        view_and_focus_client(socket_path, client.id, target_tag)?;
        self.focused_id = Some(client.id);

        Ok(format!("client {} -> tag {target_tag}", client.id))
    }

    fn move_focused_to_dynamic_tag(
        &mut self,
        socket_path: &str,
        client: &Client,
        target_tag: u32,
    ) -> Result<String> {
        if static_tag(&client.appid).is_some() {
            let pinned_tag = static_tag(&client.appid).unwrap();
            return Err(format!("{} is pinned to tag {pinned_tag}", client.appid).into());
        }

        let source_tag = single_tag(client)
            .ok_or_else(|| format!("client {} is assigned to multiple tags", client.id))?;

        if source_tag == target_tag {
            return Ok(format!("client {} already on tag {target_tag}", client.id));
        }

        let destination_clients = self.clients_on_tag(target_tag, client.id);

        if destination_clients.len() > 1 {
            return Err(format!(
                "tag {target_tag} contains multiple clients; refusing ambiguous swap"
            )
            .into());
        }

        if let Some(occupant) = destination_clients.first() {
            if !(DYNAMIC_TAG_FIRST..=DYNAMIC_TAG_LAST).contains(&source_tag) {
                return Err(format!(
                    "cannot swap tag {target_tag} into reserved tag {source_tag}; move the destination window explicitly instead"
                )
                .into());
            }

            let source_clients = self.clients_on_tag(source_tag, client.id);
            if !source_clients.is_empty() {
                return Err(format!(
                    "source tag {source_tag} contains multiple clients; refusing ambiguous swap"
                )
                .into());
            }

            if static_tag(&occupant.appid).is_some() {
                return Err(
                    format!("tag {target_tag} contains pinned client {}", occupant.appid).into(),
                );
            }

            move_silent(socket_path, occupant.id, source_tag)?;
            move_silent(socket_path, client.id, target_tag)?;

            if let Some(current) = self.clients.get_mut(&occupant.id) {
                current.tags = vec![source_tag];
            }
            if let Some(current) = self.clients.get_mut(&client.id) {
                current.tags = vec![target_tag];
            }

            self.manual_static_stack.remove(&client.id);
            self.overflow.remove(&client.id);
            self.pending.remove(&client.id);
            self.managed.insert(client.id);
            self.touch_mru(client.id);

            // If the occupant was overflow/manual, moving it into the dynamic
            // pool makes it managed again.
            self.manual_static_stack.remove(&occupant.id);
            self.overflow.remove(&occupant.id);
            self.pending.remove(&occupant.id);
            self.managed.insert(occupant.id);

            self.persist_state()?;
            view_and_focus_client(socket_path, client.id, target_tag)?;
            self.focused_id = Some(client.id);

            Ok(format!(
                "swapped client {} tag {source_tag} <-> client {} tag {target_tag}",
                client.id, occupant.id
            ))
        } else {
            move_silent(socket_path, client.id, target_tag)?;

            if let Some(current) = self.clients.get_mut(&client.id) {
                current.tags = vec![target_tag];
            }

            self.manual_static_stack.remove(&client.id);
            self.overflow.remove(&client.id);
            self.pending.remove(&client.id);
            self.managed.insert(client.id);
            self.touch_mru(client.id);
            self.persist_state()?;

            view_and_focus_client(socket_path, client.id, target_tag)?;
            self.focused_id = Some(client.id);

            Ok(format!("client {} -> tag {target_tag}", client.id))
        }
    }

    fn move_focused_to(&mut self, socket_path: &str, target_tag: u32) -> Result<String> {
        if !(STATIC_FOOT_TAG..=DYNAMIC_TAG_LAST).contains(&target_tag) {
            return Err(format!("tag must be between 1 and {DYNAMIC_TAG_LAST}").into());
        }

        self.refresh_snapshot(socket_path)?;
        let client = self.focused_client()?;

        if !on_managed_monitor(&client) {
            return Err(format!("focused client {} is not on {MANAGED_MONITOR}", client.id).into());
        }

        if target_tag <= STATIC_EMACS_TAG {
            self.move_focused_to_static_tag(socket_path, &client, target_tag)
        } else {
            self.move_focused_to_dynamic_tag(socket_path, &client, target_tag)
        }
    }

    fn move_focused_relative(&mut self, socket_path: &str, next: bool) -> Result<String> {
        self.refresh_snapshot(socket_path)?;
        let client = self.focused_client()?;

        if static_tag(&client.appid).is_some() {
            return Err(format!("{} is pinned and cannot be pushed", client.appid).into());
        }

        let source_tag = single_dynamic_tag(&client).ok_or_else(|| {
            format!(
                "focused client {} must be on a dynamic tag ({DYNAMIC_TAG_FIRST}-{DYNAMIC_TAG_LAST})",
                client.id
            )
        })?;

        let target_tag = if next {
            if source_tag == DYNAMIC_TAG_LAST {
                DYNAMIC_TAG_FIRST
            } else {
                source_tag + 1
            }
        } else if source_tag == DYNAMIC_TAG_FIRST {
            DYNAMIC_TAG_LAST
        } else {
            source_tag - 1
        };

        self.move_focused_to_dynamic_tag(socket_path, &client, target_tag)
    }

    fn handle_control(&mut self, socket_path: &str, command: ControlCommand) -> Result<String> {
        match command {
            ControlCommand::Next => self.move_focused_relative(socket_path, true),
            ControlCommand::Prev => self.move_focused_relative(socket_path, false),
            ControlCommand::To(tag) => self.move_focused_to(socket_path, tag),
        }
    }
}

fn parse_control_command(line: &str) -> std::result::Result<ControlCommand, String> {
    let mut parts = line.split_whitespace();

    match parts.next() {
        Some("move-next") if parts.next().is_none() => Ok(ControlCommand::Next),
        Some("move-prev") if parts.next().is_none() => Ok(ControlCommand::Prev),
        Some("move-to") => {
            let tag = parts
                .next()
                .ok_or_else(|| "move-to requires a tag number".to_string())?
                .parse::<u32>()
                .map_err(|_| "tag must be a number".to_string())?;

            if parts.next().is_some() {
                return Err("move-to accepts exactly one tag number".to_string());
            }

            if !(STATIC_FOOT_TAG..=DYNAMIC_TAG_LAST).contains(&tag) {
                return Err(format!("tag must be between 1 and {DYNAMIC_TAG_LAST}"));
            }

            Ok(ControlCommand::To(tag))
        }
        _ => Err("expected: move-next | move-prev | move-to <1-10>".to_string()),
    }
}

fn run_control_client(command: String) -> Result<()> {
    let socket_path = control_socket_path()?;
    let mut stream = UnixStream::connect(&socket_path).map_err(|err| {
        format!(
            "unable to connect to mango-dynamic-tags daemon at {}: {err}",
            socket_path.display()
        )
    })?;

    stream.write_all(command.as_bytes())?;
    stream.write_all(b"\n")?;
    stream.flush()?;

    let mut response = String::new();
    BufReader::new(stream).read_to_string(&mut response)?;

    let response = response.trim();

    if let Some(message) = response.strip_prefix("ok ") {
        if !message.is_empty() {
            println!("{message}");
        }
        return Ok(());
    }

    if let Some(message) = response.strip_prefix("error ") {
        return Err(message.to_string().into());
    }

    Err(format!("unexpected daemon response: {response}").into())
}

fn spawn_control_listener(
    socket_path: PathBuf,
    tx: Sender<Event>,
) -> Result<thread::JoinHandle<()>> {
    if socket_path.exists() {
        match UnixStream::connect(&socket_path) {
            Ok(_) => {
                return Err(
                    format!("control socket already active: {}", socket_path.display()).into(),
                );
            }
            Err(_) => {
                fs::remove_file(&socket_path)?;
            }
        }
    }

    let listener = UnixListener::bind(&socket_path)?;

    Ok(thread::spawn(move || {
        for incoming in listener.incoming() {
            let Ok(mut stream) = incoming else {
                continue;
            };

            let mut line = String::new();
            let read_result = {
                let mut reader = BufReader::new(&mut stream);
                reader.read_line(&mut line)
            };

            if let Err(err) = read_result {
                let _ = writeln!(stream, "error unable to read command: {err}");
                continue;
            }

            let command = match parse_control_command(line.trim()) {
                Ok(command) => command,
                Err(err) => {
                    let _ = writeln!(stream, "error {err}");
                    continue;
                }
            };

            let (reply_tx, reply_rx) = mpsc::channel();

            if tx
                .send(Event::Control(ControlRequest {
                    command,
                    reply: reply_tx,
                }))
                .is_err()
            {
                let _ = writeln!(stream, "error daemon event loop is unavailable");
                continue;
            }

            match reply_rx.recv() {
                Ok(Ok(message)) => {
                    let _ = writeln!(stream, "ok {message}");
                }
                Ok(Err(message)) => {
                    let _ = writeln!(stream, "error {message}");
                }
                Err(_) => {
                    let _ = writeln!(stream, "error daemon reply channel closed");
                }
            }
        }
    }))
}

fn watch_stream(
    socket_path: &str,
    name: &'static str,
    command: &str,
    tx: &Sender<Event>,
) -> std::result::Result<(), String> {
    let mut stream = UnixStream::connect(socket_path).map_err(|err| err.to_string())?;

    stream
        .write_all(command.as_bytes())
        .map_err(|err| err.to_string())?;
    stream.write_all(b"\n").map_err(|err| err.to_string())?;
    stream.flush().map_err(|err| err.to_string())?;

    let reader = BufReader::new(stream);

    for line in reader.lines() {
        let line = line.map_err(|err| err.to_string())?;

        if line.trim().is_empty() {
            continue;
        }

        match name {
            "all-clients" => {
                if tx.send(Event::ClientsChanged).is_err() {
                    return Ok(());
                }
            }
            "focusing-client" => {
                if line.trim() == "null" {
                    continue;
                }

                match serde_json::from_str::<Client>(&line) {
                    Ok(client) => {
                        if tx.send(Event::Focus(client)).is_err() {
                            return Ok(());
                        }
                    }
                    Err(err) => {
                        let _ = tx.send(Event::WatcherError(
                            name,
                            format!("unable to parse event: {err}: {line}"),
                        ));
                    }
                }
            }
            _ => return Err(format!("unknown watcher type: {name}")),
        }
    }

    Ok(())
}

fn spawn_watch(
    socket_path: String,
    name: &'static str,
    command: &'static str,
    tx: Sender<Event>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        if let Err(err) = watch_stream(&socket_path, name, command, &tx) {
            let _ = tx.send(Event::WatcherError(name, err));
        }

        let _ = tx.send(Event::WatcherEnded(name));
    })
}

fn event_loop(socket_path: &str, state: &mut DynamicTags, rx: Receiver<Event>) -> Result<()> {
    while let Ok(event) = rx.recv() {
        match event {
            Event::ClientsChanged => {
                let clients = get_clients(socket_path)?;
                state.handle_clients(socket_path, clients)?;
            }
            Event::Focus(client) => {
                state.handle_focus(client);
            }
            Event::Control(request) => {
                let result = state
                    .handle_control(socket_path, request.command)
                    .map_err(|err| err.to_string());
                let _ = request.reply.send(result);
            }
            Event::WatcherError(name, message) => {
                eprintln!("mango-dynamic-tags: {name} watcher: {message}");
            }
            Event::WatcherEnded(name) => {
                return Err(format!("{name} watcher exited").into());
            }
        }
    }

    Err("event channel closed".into())
}

fn run_daemon() -> Result<()> {
    let mango_socket =
        env::var("MANGO_INSTANCE_SIGNATURE").map_err(|_| "MANGO_INSTANCE_SIGNATURE is not set")?;

    if !Path::new(&mango_socket).exists() {
        return Err(format!("Mango IPC socket does not exist: {mango_socket}").into());
    }

    validate_tag_capacity(&mango_socket)?;

    let control_path = control_socket_path()?;
    let state_path = state_file_path()?;

    eprintln!("mango-dynamic-tags: using {mango_socket}");
    eprintln!(
        "mango-dynamic-tags: control socket {}",
        control_path.display()
    );

    let (tx, rx) = mpsc::channel();

    let _control_listener = spawn_control_listener(control_path.clone(), tx.clone())?;

    let _clients_watch = spawn_watch(
        mango_socket.clone(),
        "all-clients",
        "watch all-clients",
        tx.clone(),
    );

    let _focus_watch = spawn_watch(
        mango_socket.clone(),
        "focusing-client",
        "watch focusing-client",
        tx,
    );

    let mut state = DynamicTags::new(mango_socket.clone(), state_path);
    state.initial_reconcile(&mango_socket)?;

    let clients = get_clients(&mango_socket)?;
    state.handle_clients(&mango_socket, clients)?;

    eprintln!("mango-dynamic-tags: started; Foot=1, Emacs=2, dynamic=3-10, wrap enabled");

    let result = event_loop(&mango_socket, &mut state, rx);
    let _ = fs::remove_file(control_path);
    result
}

fn usage() {
    eprintln!("usage:");
    eprintln!("  mango-dynamic-tags");
    eprintln!("  mango-dynamic-tags move-next");
    eprintln!("  mango-dynamic-tags move-prev");
    eprintln!("  mango-dynamic-tags move-to <1-10>");
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();

    if args.is_empty() {
        return run_daemon();
    }

    let command_line = args.join(" ");

    if parse_control_command(&command_line).is_err() {
        usage();
        return Err("invalid command".into());
    }

    run_control_client(command_line)
}
