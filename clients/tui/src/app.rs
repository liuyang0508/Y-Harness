//! Product TUI state derived exclusively from Protocol v20 projections.

use std::{
    collections::{BTreeSet, VecDeque},
    io,
    time::{Duration, Instant},
};

use ratatui::crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use y_harness::{
    ApprovalRecord, ItemKind, MemoryScope, ModelStreamEvent, OperationId, OperationStatus,
    ProtocolCommand, ProtocolResult, StateCapacity, StateEvent, StoredEvent, TaskGraphSummary,
    TaskRecord, Thread, ThreadId, ThreadSummary, TurnStatus,
};

use crate::{
    protocol::{ClientResult, ProtocolClient},
    ui::{Tui, terminal_unsafe},
};

const MAX_COMPOSER_BYTES: usize = 65_536;
const MAX_PROVISIONAL_BYTES: usize = 65_536;
const MAX_ACTIVITY: usize = 256;
const OPERATION_POLL_INTERVAL: Duration = Duration::from_millis(40);
const ACTIVE_REFRESH_INTERVAL: Duration = Duration::from_millis(400);
const IDLE_REFRESH_INTERVAL: Duration = Duration::from_secs(2);
const ACTIVE_EVENT_WAIT: Duration = Duration::from_millis(25);
const IDLE_EVENT_WAIT: Duration = Duration::from_millis(250);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Focus {
    Composer,
    Sidebar,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SidebarTab {
    Activity,
    Sessions,
    Approvals,
    Tasks,
}

impl SidebarTab {
    pub(crate) const ALL: [Self; 4] =
        [Self::Activity, Self::Sessions, Self::Approvals, Self::Tasks];

    fn next(self) -> Self {
        match self {
            Self::Activity => Self::Sessions,
            Self::Sessions => Self::Approvals,
            Self::Approvals => Self::Tasks,
            Self::Tasks => Self::Activity,
        }
    }

    fn previous(self) -> Self {
        match self {
            Self::Activity => Self::Tasks,
            Self::Sessions => Self::Activity,
            Self::Approvals => Self::Sessions,
            Self::Tasks => Self::Approvals,
        }
    }
}

pub(crate) struct ActiveTurn {
    pub(crate) id: OperationId,
    pub(crate) provisional: String,
    pub(crate) provisional_truncated: bool,
    pub(crate) stream_gap_through: Option<u64>,
    pub(crate) started_at: Instant,
    cursor: u64,
}

pub(crate) struct Notice {
    pub(crate) text: String,
    pub(crate) error: bool,
}

pub(crate) struct ActivityEntry {
    pub(crate) sequence: u64,
    pub(crate) text: String,
}

pub(crate) struct App {
    pub(crate) server: String,
    pub(crate) engine_version: String,
    pub(crate) capabilities: BTreeSet<String>,
    pub(crate) thread: Thread,
    pub(crate) capacity: Option<StateCapacity>,
    pub(crate) activity: VecDeque<ActivityEntry>,
    pub(crate) sessions: Vec<ThreadSummary>,
    pub(crate) sessions_have_more: bool,
    pub(crate) selected_session: usize,
    pub(crate) approvals: Vec<ApprovalRecord>,
    pub(crate) selected_approval: usize,
    pub(crate) graph_id: Option<String>,
    pub(crate) graph: Option<TaskGraphSummary>,
    pub(crate) tasks: Vec<TaskRecord>,
    pub(crate) tasks_have_more: bool,
    pub(crate) selected_task: usize,
    pub(crate) input: String,
    pub(crate) input_cursor: usize,
    pub(crate) transcript_scroll_from_bottom: usize,
    pub(crate) focus: Focus,
    pub(crate) sidebar_tab: SidebarTab,
    pub(crate) active: Option<ActiveTurn>,
    pub(crate) notice: Notice,
    pub(crate) help: bool,
    pub(crate) quit: bool,
    quit_after_settlement: bool,
    event_cursor: u64,
    last_operation_poll: Instant,
    last_refresh: Instant,
}

impl App {
    pub(crate) async fn bootstrap(
        client: &mut ProtocolClient,
        initial_thread: Option<String>,
    ) -> ClientResult<Self> {
        let (server, capabilities, engine_version) =
            match client.call(ProtocolCommand::Initialize {}).await? {
                ProtocolResult::Initialized {
                    server,
                    capabilities,
                    compatibility,
                } => (
                    server,
                    capabilities.into_iter().collect::<BTreeSet<_>>(),
                    compatibility.engine_version,
                ),
                result => return Err(unexpected("initialize", result)),
            };
        for required in [
            "operation.cancel",
            "operation.events",
            "operation.forget",
            "operation.get",
            "thread.capacity",
            "thread.create",
            "thread.events",
            "thread.get",
            "thread.name",
            "turn.start",
            "turn.steer",
        ] {
            if !capabilities.contains(required) {
                return Err(io::Error::other(format!(
                    "Engine did not advertise required capability {required:?}"
                ))
                .into());
            }
        }
        let thread = match initial_thread {
            Some(thread_id) => load_thread(client, thread_id).await?,
            None => create_thread(client).await?,
        };
        let now = Instant::now();
        let mut app = Self {
            server,
            engine_version,
            capabilities,
            thread,
            capacity: None,
            activity: VecDeque::new(),
            sessions: Vec::new(),
            sessions_have_more: false,
            selected_session: 0,
            approvals: Vec::new(),
            selected_approval: 0,
            graph_id: None,
            graph: None,
            tasks: Vec::new(),
            tasks_have_more: false,
            selected_task: 0,
            input: String::new(),
            input_cursor: 0,
            transcript_scroll_from_bottom: 0,
            focus: Focus::Composer,
            sidebar_tab: SidebarTab::Activity,
            active: None,
            notice: Notice {
                text: "Ready · Enter sends · F1 help".to_owned(),
                error: false,
            },
            help: false,
            quit: false,
            quit_after_settlement: false,
            event_cursor: 0,
            last_operation_poll: now,
            last_refresh: now,
        };
        app.refresh_all(client).await;
        Ok(app)
    }

    pub(crate) async fn run(
        &mut self,
        terminal: &mut Tui,
        client: &mut ProtocolClient,
    ) -> ClientResult<()> {
        terminal.draw(|frame| crate::ui::render(frame, self))?;
        while !self.quit {
            let mut redraw = false;
            let now = Instant::now();
            if self.active.is_some()
                && now.duration_since(self.last_operation_poll) >= OPERATION_POLL_INTERVAL
            {
                if let Err(error) = self.poll_active(client).await {
                    self.set_error(error.to_string());
                }
                self.last_operation_poll = now;
                redraw = true;
            }
            let refresh_interval = if self.active.is_some() {
                ACTIVE_REFRESH_INTERVAL
            } else {
                IDLE_REFRESH_INTERVAL
            };
            if now.duration_since(self.last_refresh) >= refresh_interval {
                self.refresh_all(client).await;
                self.last_refresh = now;
                redraw = true;
            }

            let event_wait = if self.active.is_some() {
                ACTIVE_EVENT_WAIT
            } else {
                IDLE_EVENT_WAIT
            };
            if event::poll(event_wait)? {
                match event::read()? {
                    Event::Key(key)
                        if matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) =>
                    {
                        if let Err(error) = self.handle_key(client, key).await {
                            self.set_error(error.to_string());
                        }
                    }
                    Event::Paste(text) if self.focus == Focus::Composer && !self.help => {
                        self.insert_text(&text);
                    }
                    Event::Resize(_, _) | Event::FocusGained | Event::FocusLost => {}
                    _ => {}
                }
                redraw = true;
            }
            if redraw && !self.quit {
                terminal.draw(|frame| crate::ui::render(frame, self))?;
            }
        }
        Ok(())
    }

    async fn handle_key(&mut self, client: &mut ProtocolClient, key: KeyEvent) -> ClientResult<()> {
        if self.help {
            if matches!(key.code, KeyCode::Esc | KeyCode::F(1) | KeyCode::Char('?')) {
                self.help = false;
            }
            return Ok(());
        }
        if key.code == KeyCode::F(1) || key.code == KeyCode::Char('?') && self.input.is_empty() {
            self.help = true;
            return Ok(());
        }
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            if self.active.is_some() {
                self.cancel_active(client).await?;
            } else {
                self.quit = true;
            }
            return Ok(());
        }
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('n') {
            self.create_and_switch_thread(client).await?;
            return Ok(());
        }
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('r') {
            self.refresh_all(client).await;
            self.set_notice("Protocol projections refreshed");
            return Ok(());
        }
        match key.code {
            KeyCode::PageUp => {
                self.transcript_scroll_from_bottom =
                    self.transcript_scroll_from_bottom.saturating_add(8);
                return Ok(());
            }
            KeyCode::PageDown => {
                self.transcript_scroll_from_bottom =
                    self.transcript_scroll_from_bottom.saturating_sub(8);
                return Ok(());
            }
            KeyCode::Tab => {
                self.focus = match self.focus {
                    Focus::Composer => Focus::Sidebar,
                    Focus::Sidebar => Focus::Composer,
                };
                return Ok(());
            }
            KeyCode::BackTab => {
                self.focus = match self.focus {
                    Focus::Composer => Focus::Sidebar,
                    Focus::Sidebar => Focus::Composer,
                };
                return Ok(());
            }
            KeyCode::Esc => {
                if self.active.is_some() {
                    self.cancel_active(client).await?;
                } else if !self.input.is_empty() {
                    self.input.clear();
                    self.input_cursor = 0;
                } else {
                    self.focus = Focus::Composer;
                }
                return Ok(());
            }
            _ => {}
        }

        match self.focus {
            Focus::Composer => self.handle_composer_key(client, key).await,
            Focus::Sidebar => self.handle_sidebar_key(client, key).await,
        }
    }

    async fn handle_composer_key(
        &mut self,
        client: &mut ProtocolClient,
        key: KeyEvent,
    ) -> ClientResult<()> {
        match key.code {
            KeyCode::Enter
                if key
                    .modifiers
                    .intersects(KeyModifiers::ALT | KeyModifiers::CONTROL) =>
            {
                self.insert_text("\n");
            }
            KeyCode::Enter => self.submit(client).await?,
            KeyCode::Backspace => self.backspace(),
            KeyCode::Delete => self.delete(),
            KeyCode::Left => self.input_cursor = self.input_cursor.saturating_sub(1),
            KeyCode::Right => {
                self.input_cursor = self
                    .input_cursor
                    .saturating_add(1)
                    .min(self.input.chars().count());
            }
            KeyCode::Home => self.input_cursor = 0,
            KeyCode::End => self.input_cursor = self.input.chars().count(),
            KeyCode::Char(character)
                if !key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
            {
                self.insert_text(&character.to_string());
            }
            _ => {}
        }
        Ok(())
    }

    async fn handle_sidebar_key(
        &mut self,
        client: &mut ProtocolClient,
        key: KeyEvent,
    ) -> ClientResult<()> {
        match key.code {
            KeyCode::Left => self.sidebar_tab = self.sidebar_tab.previous(),
            KeyCode::Right => self.sidebar_tab = self.sidebar_tab.next(),
            KeyCode::Up => match self.sidebar_tab {
                SidebarTab::Sessions => {
                    self.selected_session = self.selected_session.saturating_sub(1);
                }
                SidebarTab::Approvals => {
                    self.selected_approval = self.selected_approval.saturating_sub(1);
                }
                SidebarTab::Tasks => {
                    self.selected_task = self.selected_task.saturating_sub(1);
                }
                SidebarTab::Activity => {}
            },
            KeyCode::Down => match self.sidebar_tab {
                SidebarTab::Sessions => {
                    self.selected_session = self
                        .selected_session
                        .saturating_add(1)
                        .min(self.sessions.len().saturating_sub(1));
                }
                SidebarTab::Approvals => {
                    self.selected_approval = self
                        .selected_approval
                        .saturating_add(1)
                        .min(self.approvals.len().saturating_sub(1));
                }
                SidebarTab::Tasks => {
                    self.selected_task = self
                        .selected_task
                        .saturating_add(1)
                        .min(self.tasks.len().saturating_sub(1));
                }
                SidebarTab::Activity => {}
            },
            KeyCode::Enter if self.sidebar_tab == SidebarTab::Sessions => {
                let thread_id = self
                    .sessions
                    .get(self.selected_session)
                    .map(|summary| summary.thread_id.to_string())
                    .ok_or_else(|| io::Error::other("No Thread selected"))?;
                self.switch_thread(client, thread_id).await?;
            }
            KeyCode::Char('r') => {
                self.refresh_all(client).await;
                self.set_notice("Protocol projections refreshed");
            }
            _ => {}
        }
        Ok(())
    }

    async fn submit(&mut self, client: &mut ProtocolClient) -> ClientResult<()> {
        let submitted = self.input.trim().to_owned();
        if submitted.is_empty() {
            return Ok(());
        }
        if submitted.starts_with('/') {
            self.input.clear();
            self.input_cursor = 0;
            return self.run_command(client, &submitted).await;
        }
        if self.active.is_some() {
            self.refresh_thread(client).await?;
            let mut running = self
                .thread
                .turns
                .iter()
                .filter(|turn| turn.status == TurnStatus::Running);
            let turn_id = running
                .next()
                .map(|turn| turn.id.clone())
                .ok_or_else(|| io::Error::other("active operation has not created its Turn yet"))?;
            if running.next().is_some() {
                return Err(io::Error::other("Thread contains multiple running Turns").into());
            }
            match client
                .call(ProtocolCommand::SteerTurn {
                    thread_id: self.thread.id.to_string(),
                    expected_turn_id: turn_id.to_string(),
                    content: submitted,
                })
                .await?
            {
                ProtocolResult::TurnSteered { .. } => {
                    self.input.clear();
                    self.input_cursor = 0;
                    self.set_notice("Steering durably queued for the active Turn");
                }
                result => return Err(unexpected("steer Turn", result)),
            }
            return Ok(());
        }
        let result = client
            .call(ProtocolCommand::StartTurn {
                thread_id: self.thread.id.to_string(),
                prompt: submitted,
                memory_scope: MemoryScope::default(),
                context: Vec::new(),
                timeout_ms: Some(120_000),
            })
            .await?;
        let operation_id = match result {
            ProtocolResult::TurnStarted { operation_id } => operation_id,
            result => return Err(unexpected("start Turn", result)),
        };
        self.input.clear();
        self.input_cursor = 0;
        self.transcript_scroll_from_bottom = 0;
        self.active = Some(ActiveTurn {
            id: operation_id,
            provisional: String::new(),
            provisional_truncated: false,
            stream_gap_through: None,
            started_at: Instant::now(),
            cursor: 0,
        });
        self.last_operation_poll = Instant::now();
        self.set_notice("Turn running · Esc cancels cooperatively");
        Ok(())
    }

    async fn run_command(
        &mut self,
        client: &mut ProtocolClient,
        command: &str,
    ) -> ClientResult<()> {
        let mut parts = command.split_whitespace();
        match parts.next().unwrap_or_default() {
            "/new" => self.create_and_switch_thread(client).await,
            "/fork" => {
                let through_turn_id = parts.next().map(str::to_owned);
                if parts.next().is_some() {
                    return Err(io::Error::other("usage: /fork [terminal-turn-id]").into());
                }
                self.fork_and_switch_thread(client, through_turn_id).await
            }
            "/name" => {
                let name = command.strip_prefix("/name").unwrap_or_default().trim();
                let name = (!name.is_empty()).then(|| name.to_owned());
                match client
                    .call(ProtocolCommand::SetThreadName {
                        thread_id: self.thread.id.to_string(),
                        name: name.clone(),
                    })
                    .await?
                {
                    ProtocolResult::ThreadNamed { name } => {
                        self.thread.name = name;
                        self.refresh_sessions(client).await?;
                        self.set_notice(if self.thread.name.is_some() {
                            "Thread name changed"
                        } else {
                            "Thread name cleared"
                        });
                        Ok(())
                    }
                    result => Err(unexpected("name Thread", result)),
                }
            }
            "/thread" => {
                let thread_id = parts
                    .next()
                    .ok_or_else(|| io::Error::other("usage: /thread <thread-id>"))?;
                if parts.next().is_some() {
                    return Err(io::Error::other("usage: /thread <thread-id>").into());
                }
                self.switch_thread(client, thread_id.to_owned()).await
            }
            "/graph" => {
                let graph_id = parts
                    .next()
                    .ok_or_else(|| io::Error::other("usage: /graph <graph-id>"))?;
                if parts.next().is_some() {
                    return Err(io::Error::other("usage: /graph <graph-id>").into());
                }
                self.graph_id = Some(graph_id.to_owned());
                self.sidebar_tab = SidebarTab::Tasks;
                self.focus = Focus::Sidebar;
                self.refresh_task(client).await?;
                self.set_notice(format!("Watching Task Graph {graph_id}"));
                Ok(())
            }
            "/events" => {
                self.sidebar_tab = SidebarTab::Activity;
                self.focus = Focus::Sidebar;
                self.refresh_events(client).await?;
                Ok(())
            }
            "/sessions" => {
                self.sidebar_tab = SidebarTab::Sessions;
                self.focus = Focus::Sidebar;
                self.refresh_sessions(client).await?;
                Ok(())
            }
            "/approvals" => {
                self.sidebar_tab = SidebarTab::Approvals;
                self.focus = Focus::Sidebar;
                self.refresh_approvals(client).await?;
                Ok(())
            }
            "/tasks" => {
                self.sidebar_tab = SidebarTab::Tasks;
                self.focus = Focus::Sidebar;
                Ok(())
            }
            "/refresh" => {
                self.refresh_all(client).await;
                self.set_notice("Protocol projections refreshed");
                Ok(())
            }
            "/cancel" => self.cancel_active(client).await,
            "/help" => {
                self.help = true;
                Ok(())
            }
            "/quit" | "/exit" => {
                if self.active.is_some() {
                    self.quit_after_settlement = true;
                    self.cancel_active(client).await?;
                    self.set_notice("Cancelling the active Turn before exit");
                } else {
                    self.quit = true;
                }
                Ok(())
            }
            unknown => {
                self.set_error(format!("Unknown TUI command {unknown:?}; F1 shows help"));
                Ok(())
            }
        }
    }

    async fn create_and_switch_thread(&mut self, client: &mut ProtocolClient) -> ClientResult<()> {
        if self.active.is_some() {
            return Err(io::Error::other("cancel the running Turn before switching Thread").into());
        }
        let thread = create_thread(client).await?;
        self.install_thread(thread);
        self.refresh_all(client).await;
        self.set_notice("Created a new authoritative Thread");
        Ok(())
    }

    async fn fork_and_switch_thread(
        &mut self,
        client: &mut ProtocolClient,
        through_turn_id: Option<String>,
    ) -> ClientResult<()> {
        if self.active.is_some() {
            return Err(io::Error::other("cancel the running Turn before forking").into());
        }
        if !self.capabilities.contains("thread.fork") {
            return Err(io::Error::other("Engine does not advertise atomic Thread fork").into());
        }
        let child_thread_id = ThreadId::generate();
        let parent_thread_id = self.thread.id.clone();
        let thread = match client
            .call(ProtocolCommand::ForkThread {
                parent_thread_id: parent_thread_id.to_string(),
                child_thread_id: child_thread_id.to_string(),
                through_turn_id,
            })
            .await?
        {
            ProtocolResult::ThreadForked { thread } => thread,
            result => return Err(unexpected("fork Thread", result)),
        };
        self.install_thread(thread);
        self.refresh_all(client).await;
        self.set_notice(format!(
            "Forked {} into independent Thread {}",
            short_id(parent_thread_id.as_str()),
            short_id(child_thread_id.as_str())
        ));
        Ok(())
    }

    async fn switch_thread(
        &mut self,
        client: &mut ProtocolClient,
        thread_id: String,
    ) -> ClientResult<()> {
        if self.active.is_some() {
            return Err(io::Error::other("cancel the running Turn before switching Thread").into());
        }
        let thread = load_thread(client, thread_id).await?;
        self.install_thread(thread);
        self.refresh_all(client).await;
        self.set_notice("Attached to authoritative Thread");
        Ok(())
    }

    fn install_thread(&mut self, thread: Thread) {
        self.thread = thread;
        self.capacity = None;
        self.activity.clear();
        self.event_cursor = 0;
        self.transcript_scroll_from_bottom = 0;
        self.active = None;
    }

    async fn cancel_active(&mut self, client: &mut ProtocolClient) -> ClientResult<()> {
        let Some(active) = &self.active else {
            self.set_notice("No running Turn");
            return Ok(());
        };
        match client
            .call(ProtocolCommand::CancelOperation {
                operation_id: active.id.to_string(),
            })
            .await?
        {
            ProtocolResult::Cancellation { accepted, .. } => {
                if accepted {
                    self.set_notice("Cancellation requested; waiting for durable settlement");
                } else {
                    self.set_notice("Operation already settled");
                }
                Ok(())
            }
            result => Err(unexpected("cancel operation", result)),
        }
    }

    async fn poll_active(&mut self, client: &mut ProtocolClient) -> ClientResult<()> {
        let Some(active) = &self.active else {
            return Ok(());
        };
        let operation_id = active.id.clone();
        for _ in 0..4 {
            let cursor = self.active.as_ref().map_or(0, |active| active.cursor);
            let result = client
                .call(ProtocolCommand::GetOperationEvents {
                    operation_id: operation_id.to_string(),
                    after_sequence: Some(cursor),
                    limit: Some(32),
                })
                .await?;
            let (events, next, has_more, dropped) = match result {
                ProtocolResult::OperationEvents {
                    events,
                    next_after_sequence,
                    has_more,
                    dropped_through_sequence,
                } => (
                    events,
                    next_after_sequence,
                    has_more,
                    dropped_through_sequence,
                ),
                result => return Err(unexpected("read operation events", result)),
            };
            let Some(active) = self.active.as_mut() else {
                return Ok(());
            };
            if let Some(dropped) = dropped
                && active.cursor < dropped
            {
                active.stream_gap_through = Some(dropped);
                active.cursor = dropped;
                active.provisional.clear();
                active.provisional_truncated = false;
            }
            for event in events {
                match event.event {
                    ModelStreamEvent::TextDelta { delta, .. } => {
                        append_provisional(active, &delta);
                    }
                    ModelStreamEvent::StepInvalidated { .. } => {
                        active.provisional.clear();
                        active.provisional_truncated = false;
                    }
                }
                active.cursor = event.sequence;
            }
            if let Some(next) = next {
                active.cursor = next;
            }
            if !has_more {
                break;
            }
        }

        match client
            .call(ProtocolCommand::GetOperation {
                operation_id: operation_id.to_string(),
            })
            .await?
        {
            ProtocolResult::Operation {
                operation: OperationStatus::Running { .. },
            } => {}
            ProtocolResult::Operation {
                operation: OperationStatus::Completed { .. },
            } => {
                self.finish_operation(client, &operation_id).await;
                self.set_notice("Turn completed");
            }
            ProtocolResult::Operation {
                operation: OperationStatus::Failed { error },
            }
            | ProtocolResult::Operation {
                operation: OperationStatus::Cancelled { error },
            }
            | ProtocolResult::Operation {
                operation: OperationStatus::TimedOut { error },
            } => {
                self.finish_operation(client, &operation_id).await;
                self.set_error(error);
            }
            result => return Err(unexpected("poll operation", result)),
        }
        Ok(())
    }

    async fn finish_operation(&mut self, client: &mut ProtocolClient, operation_id: &OperationId) {
        let _ = client
            .call(ProtocolCommand::ForgetOperation {
                operation_id: operation_id.to_string(),
            })
            .await;
        self.active = None;
        self.refresh_all(client).await;
        self.transcript_scroll_from_bottom = 0;
        if self.quit_after_settlement {
            self.quit = true;
        }
    }

    async fn refresh_all(&mut self, client: &mut ProtocolClient) {
        let mut errors = Vec::new();
        if let Err(error) = self.refresh_thread(client).await {
            errors.push(error.to_string());
        }
        if let Err(error) = self.refresh_events(client).await {
            errors.push(error.to_string());
        }
        if let Err(error) = self.refresh_sessions(client).await {
            errors.push(error.to_string());
        }
        if let Err(error) = self.refresh_capacity(client).await {
            errors.push(error.to_string());
        }
        if let Err(error) = self.refresh_approvals(client).await {
            errors.push(error.to_string());
        }
        if let Err(error) = self.refresh_task(client).await {
            errors.push(error.to_string());
        }
        if !errors.is_empty() {
            self.set_error(errors.join(" · "));
        }
    }

    async fn refresh_thread(&mut self, client: &mut ProtocolClient) -> ClientResult<()> {
        match client
            .call(ProtocolCommand::GetThread {
                thread_id: self.thread.id.to_string(),
            })
            .await?
        {
            ProtocolResult::Thread {
                thread: Some(thread),
            } => {
                self.thread = thread;
                Ok(())
            }
            ProtocolResult::Thread { thread: None } => {
                Err(io::Error::other("current Thread no longer exists").into())
            }
            result => Err(unexpected("refresh Thread", result)),
        }
    }

    async fn refresh_events(&mut self, client: &mut ProtocolClient) -> ClientResult<()> {
        for _ in 0..4 {
            let result = client
                .call(ProtocolCommand::GetEvents {
                    thread_id: self.thread.id.to_string(),
                    after_sequence: (self.event_cursor != 0).then_some(self.event_cursor),
                    limit: Some(32),
                })
                .await?;
            let (events, next, has_more) = match result {
                ProtocolResult::Events {
                    events,
                    next_after_sequence,
                    has_more,
                } => (events, next_after_sequence, has_more),
                result => return Err(unexpected("refresh events", result)),
            };
            for event in events {
                self.event_cursor = event.sequence;
                self.activity.push_back(ActivityEntry {
                    sequence: event.sequence,
                    text: describe_event(&event),
                });
                while self.activity.len() > MAX_ACTIVITY {
                    self.activity.pop_front();
                }
            }
            if let Some(next) = next {
                self.event_cursor = next;
            }
            if !has_more {
                break;
            }
        }
        Ok(())
    }

    async fn refresh_capacity(&mut self, client: &mut ProtocolClient) -> ClientResult<()> {
        match client
            .call(ProtocolCommand::GetThreadCapacity {
                thread_id: self.thread.id.to_string(),
            })
            .await?
        {
            ProtocolResult::ThreadCapacity { capacity } => {
                self.capacity = Some(capacity);
                Ok(())
            }
            result => Err(unexpected("refresh Thread capacity", result)),
        }
    }

    async fn refresh_sessions(&mut self, client: &mut ProtocolClient) -> ClientResult<()> {
        if !self.capabilities.contains("thread.list") {
            self.sessions.clear();
            self.sessions_have_more = false;
            self.selected_session = 0;
            return Ok(());
        }
        match client
            .call(ProtocolCommand::ListThreads {
                before_sequence: None,
                limit: Some(64),
            })
            .await?
        {
            ProtocolResult::Threads {
                threads, has_more, ..
            } => {
                self.sessions = threads;
                self.sessions_have_more = has_more;
                self.selected_session = self
                    .sessions
                    .iter()
                    .position(|summary| summary.thread_id == self.thread.id)
                    .unwrap_or_else(|| {
                        self.selected_session
                            .min(self.sessions.len().saturating_sub(1))
                    });
                Ok(())
            }
            result => Err(unexpected("refresh Threads", result)),
        }
    }

    async fn refresh_approvals(&mut self, client: &mut ProtocolClient) -> ClientResult<()> {
        if !self.capabilities.contains("approval.pending") {
            self.approvals.clear();
            return Ok(());
        }
        match client
            .call(ProtocolCommand::GetPendingApprovals { limit: Some(16) })
            .await?
        {
            ProtocolResult::PendingApprovals { approvals } => {
                self.approvals = approvals;
                self.selected_approval = self
                    .selected_approval
                    .min(self.approvals.len().saturating_sub(1));
                Ok(())
            }
            result => Err(unexpected("refresh approvals", result)),
        }
    }

    async fn refresh_task(&mut self, client: &mut ProtocolClient) -> ClientResult<()> {
        let Some(graph_id) = self.graph_id.clone() else {
            self.graph = None;
            self.tasks.clear();
            self.tasks_have_more = false;
            return Ok(());
        };
        if !self.capabilities.contains("task.graph.get") {
            return Err(io::Error::other("Engine did not advertise task.graph.get").into());
        }
        self.graph = match client
            .call(ProtocolCommand::GetTaskGraph {
                graph_id: graph_id.clone(),
            })
            .await?
        {
            ProtocolResult::TaskGraph { graph } => graph,
            result => return Err(unexpected("refresh Task Graph", result)),
        };
        if self.graph.is_none() {
            self.tasks.clear();
            self.tasks_have_more = false;
            return Ok(());
        }
        match client
            .call(ProtocolCommand::GetTaskRecords {
                graph_id,
                after_task_id: None,
                limit: Some(64),
            })
            .await?
        {
            ProtocolResult::TaskRecords { page } => {
                self.tasks = page.records;
                self.tasks_have_more = page.has_more;
                self.selected_task = self.selected_task.min(self.tasks.len().saturating_sub(1));
                Ok(())
            }
            result => Err(unexpected("refresh Task records", result)),
        }
    }

    fn insert_text(&mut self, text: &str) {
        for character in text.chars() {
            let character = if character == '\r' { '\n' } else { character };
            if character != '\n' && terminal_unsafe(character) {
                continue;
            }
            if self.input.len().saturating_add(character.len_utf8()) > MAX_COMPOSER_BYTES {
                self.set_error(format!(
                    "Composer is limited to {MAX_COMPOSER_BYTES} UTF-8 bytes"
                ));
                break;
            }
            let byte = byte_index(&self.input, self.input_cursor);
            self.input.insert(byte, character);
            self.input_cursor = self.input_cursor.saturating_add(1);
        }
    }

    fn backspace(&mut self) {
        if self.input_cursor == 0 {
            return;
        }
        let start = byte_index(&self.input, self.input_cursor - 1);
        let end = byte_index(&self.input, self.input_cursor);
        self.input.replace_range(start..end, "");
        self.input_cursor -= 1;
    }

    fn delete(&mut self) {
        if self.input_cursor >= self.input.chars().count() {
            return;
        }
        let start = byte_index(&self.input, self.input_cursor);
        let end = byte_index(&self.input, self.input_cursor + 1);
        self.input.replace_range(start..end, "");
    }

    fn set_notice(&mut self, text: impl Into<String>) {
        self.notice = Notice {
            text: text.into(),
            error: false,
        };
    }

    fn set_error(&mut self, text: impl Into<String>) {
        self.notice = Notice {
            text: text.into(),
            error: true,
        };
    }

    #[cfg(test)]
    pub(crate) fn test_fixture() -> Result<Self, y_harness::HarnessError> {
        let mut thread = Thread::new();
        let mut turn = y_harness::Turn::new(thread.id.clone());
        turn.status = TurnStatus::Completed;
        turn.items.push(y_harness::Item::new(ItemKind::UserMessage {
            content: "Design a safe Harness".to_owned(),
        }));
        turn.items
            .push(y_harness::Item::new(ItemKind::ProviderContinuation {
                model_id: "fixture/model".to_owned(),
                model_origin: y_harness::CapabilityOrigin::BuiltIn,
                continuation: y_harness::ModelContinuation::new(
                    "fixture.reasoning.v1",
                    vec![serde_json::json!({
                        "opaque": "never-render-this-ciphertext"
                    })],
                )?,
            }));
        let steering_id = y_harness::SteeringId::from_static("steering-fixture");
        turn.items
            .push(y_harness::Item::new(ItemKind::SteeringQueued {
                steering_id: steering_id.clone(),
                submitted_by: y_harness::ActorIdentity::LocalProcess,
                content: "Prefer the durable boundary.".to_owned(),
            }));
        turn.items
            .push(y_harness::Item::new(ItemKind::SteeringApplied {
                steering_id,
                content: "Prefer the durable boundary.".to_owned(),
            }));
        turn.items
            .push(y_harness::Item::new(ItemKind::AssistantMessage {
                model_id: Some("fixture/model".to_owned()),
                model_origin: None,
                content: "Keep clients behind Protocol v20.".to_owned(),
            }));
        thread.name = Some("Harness design".to_owned());
        let lineage = y_harness::ThreadLineage {
            parent_thread_id: ThreadId::from_static("fixture-parent"),
            parent_through_sequence: 1,
            parent_stream_version: 1,
            parent_events_sha256: "0".repeat(64),
        };
        thread.lineage = Some(lineage.clone());
        thread.turns.push(turn);
        let session = ThreadSummary {
            thread_id: thread.id.clone(),
            tenant_id: None,
            name: Some("Harness design".to_owned()),
            lineage: Some(lineage),
            last_sequence: 8,
            updated_at_ms: thread.created_at_ms,
            stream_version: 7,
        };
        let now = Instant::now();
        Ok(Self {
            server: "Y-Harness Engineering".to_owned(),
            engine_version: "0.1.0".to_owned(),
            capabilities: BTreeSet::from([
                "approval.pending".to_owned(),
                "task.graph.get".to_owned(),
                "thread.fork".to_owned(),
                "thread.list".to_owned(),
            ]),
            thread,
            capacity: None,
            activity: VecDeque::from([ActivityEntry {
                sequence: 1,
                text: "Thread created".to_owned(),
            }]),
            sessions: vec![session],
            sessions_have_more: false,
            selected_session: 0,
            approvals: Vec::new(),
            selected_approval: 0,
            graph_id: None,
            graph: None,
            tasks: Vec::new(),
            tasks_have_more: false,
            selected_task: 0,
            input: "下一步".to_owned(),
            input_cursor: 3,
            transcript_scroll_from_bottom: 0,
            focus: Focus::Composer,
            sidebar_tab: SidebarTab::Activity,
            active: None,
            notice: Notice {
                text: "Ready".to_owned(),
                error: false,
            },
            help: false,
            quit: false,
            quit_after_settlement: false,
            event_cursor: 1,
            last_operation_poll: now,
            last_refresh: now,
        })
    }
}

async fn create_thread(client: &mut ProtocolClient) -> ClientResult<Thread> {
    match client.call(ProtocolCommand::CreateThread {}).await? {
        ProtocolResult::ThreadCreated { thread } => Ok(thread),
        result => Err(unexpected("create Thread", result)),
    }
}

async fn load_thread(client: &mut ProtocolClient, thread_id: String) -> ClientResult<Thread> {
    match client
        .call(ProtocolCommand::GetThread { thread_id })
        .await?
    {
        ProtocolResult::Thread {
            thread: Some(thread),
        } => Ok(thread),
        ProtocolResult::Thread { thread: None } => {
            Err(io::Error::new(io::ErrorKind::NotFound, "Thread does not exist").into())
        }
        result => Err(unexpected("load Thread", result)),
    }
}

fn describe_event(event: &StoredEvent) -> String {
    match &event.event {
        StateEvent::ThreadCreated { .. } => "Thread created".to_owned(),
        StateEvent::ThreadNamed { name } => {
            if name.is_some() {
                "Thread named".to_owned()
            } else {
                "Thread name cleared".to_owned()
            }
        }
        StateEvent::ThreadForked { lineage } => format!(
            "Forked from {}",
            short_id(lineage.parent_thread_id.as_str())
        ),
        StateEvent::ThreadImported { origin } => format!(
            "Imported from {}",
            short_id(origin.source_thread_id.as_str())
        ),
        StateEvent::TurnStarted { turn_id } => {
            format!("Turn {} started", short_id(turn_id.as_str()))
        }
        StateEvent::TurnFinished { turn_id, status } => format!(
            "Turn {} {}",
            short_id(turn_id.as_str()),
            turn_status(status)
        ),
        StateEvent::CheckpointCreated { checkpoint } => format!(
            "Checkpoint {}{}",
            short_id(checkpoint.id.as_str()),
            checkpoint
                .label
                .as_deref()
                .map_or_else(String::new, |label| format!(" · {label}"))
        ),
        StateEvent::ToolCallsAppended { calls, .. } => {
            format!("Tool calls · {} ordered", calls.len())
        }
        StateEvent::ItemAppended { item, .. } => match &item.kind {
            ItemKind::UserMessage { .. } => "User message".to_owned(),
            ItemKind::SteeringQueued { .. } => "Steering queued".to_owned(),
            ItemKind::SteeringApplied { .. } => "Steering applied".to_owned(),
            ItemKind::AssistantMessage { model_id, .. } => {
                format!("Assistant · {}", model_id.as_deref().unwrap_or("legacy"))
            }
            ItemKind::ProviderContinuation {
                model_id,
                continuation,
                ..
            } => format!(
                "Provider continuation · {model_id} · {}",
                continuation.format()
            ),
            ItemKind::ToolCall { name, .. } => format!("Tool call · {name}"),
            ItemKind::PolicyDecision { .. } => "Policy decision".to_owned(),
            ItemKind::ApprovalRequested { tool, .. } => format!("Approval requested · {tool}"),
            ItemKind::ApprovalDecision { .. } => "Approval settled".to_owned(),
            ItemKind::ToolResult { is_error, .. } => {
                if *is_error {
                    "Tool failed".to_owned()
                } else {
                    "Tool completed".to_owned()
                }
            }
            ItemKind::MemoryContext { provider, .. } => format!("Memory context · {provider}"),
            ItemKind::ConversationContext { .. } => "Conversation context compiled".to_owned(),
            ItemKind::ConversationSummary { compactor, .. } => {
                format!("Conversation summary · {compactor}")
            }
            ItemKind::InvocationContext { blocks, .. } => {
                format!("Turn context · {} blocks", blocks.len())
            }
            ItemKind::RuntimeError { .. } => "Runtime error".to_owned(),
            ItemKind::TurnStopped { phase, .. } => format!("Turn stopped · {phase:?}"),
            ItemKind::VerificationResult { verifier, .. } => {
                format!("Verification · {verifier}")
            }
        },
    }
}

fn byte_index(text: &str, character_index: usize) -> usize {
    text.char_indices()
        .nth(character_index)
        .map_or(text.len(), |(index, _)| index)
}

fn append_provisional(active: &mut ActiveTurn, delta: &str) {
    if active.provisional_truncated {
        return;
    }
    let remaining = MAX_PROVISIONAL_BYTES.saturating_sub(active.provisional.len());
    if delta.len() <= remaining {
        active.provisional.push_str(delta);
        return;
    }
    let end = delta
        .char_indices()
        .take_while(|(index, character)| index.saturating_add(character.len_utf8()) <= remaining)
        .map(|(index, character)| index + character.len_utf8())
        .last()
        .unwrap_or_default();
    active.provisional.push_str(&delta[..end]);
    active.provisional_truncated = true;
}

fn short_id(identity: &str) -> &str {
    identity.rsplit('-').next().unwrap_or(identity)
}

fn turn_status(status: &TurnStatus) -> &'static str {
    match status {
        TurnStatus::Running => "running",
        TurnStatus::Completed => "completed",
        TurnStatus::Failed => "failed",
        TurnStatus::Cancelled => "cancelled",
        TurnStatus::TimedOut => "timed out",
        TurnStatus::Interrupted => "interrupted",
    }
}

fn unexpected(action: &str, result: ProtocolResult) -> Box<dyn std::error::Error + Send + Sync> {
    let _ = result;
    io::Error::other(format!("Engine returned an unexpected result for {action}")).into()
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use y_harness::OperationId;

    use super::{ActiveTurn, MAX_PROVISIONAL_BYTES, append_provisional, byte_index};

    #[test]
    fn byte_index_preserves_unicode_editing_boundaries() {
        let text = "a马尾";
        assert_eq!(byte_index(text, 0), 0);
        assert_eq!(byte_index(text, 1), 1);
        assert_eq!(byte_index(text, 2), 4);
        assert_eq!(byte_index(text, 3), 7);
    }

    #[test]
    fn provisional_projection_is_byte_bounded_on_utf8_boundaries() {
        let mut active = ActiveTurn {
            id: OperationId::generate(),
            provisional: "x".repeat(MAX_PROVISIONAL_BYTES - 1),
            provisional_truncated: false,
            stream_gap_through: None,
            started_at: Instant::now(),
            cursor: 0,
        };
        append_provisional(&mut active, "马");
        assert_eq!(active.provisional.len(), MAX_PROVISIONAL_BYTES - 1);
        assert!(active.provisional_truncated);
    }
}
