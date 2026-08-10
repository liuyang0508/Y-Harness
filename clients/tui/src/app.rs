//! Product TUI state derived exclusively from Protocol v37 projections.

use std::{
    collections::{BTreeSet, VecDeque},
    io,
    time::{Duration, Instant},
};

use ratatui::crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use sha2::{Digest, Sha256};
#[cfg(test)]
use y_harness::ProtocolAdmissionState;
use y_harness::{
    AgentLoopCloseCommandId, ApprovalDeliveryStatus, ApprovalRecord, ItemKind,
    MAX_AGENT_LOOP_WAIT_MS, MemoryScope, ModelStreamEvent, OperationId, OperationStatus,
    ProtocolCommand, ProtocolResult, ProtocolServiceStatus, RuntimeCatalog, StateCapacity,
    StateEvent, StoredEvent, TaskGraphSummary, TaskRecord, Thread, ThreadId, ThreadSummary,
    TurnExecutionProjection, TurnExecutionState, TurnId, TurnStatus,
};

use crate::{
    protocol::{ClientResult, ProtocolClient},
    ui::{Tui, terminal_unsafe},
};

const MAX_COMPOSER_BYTES: usize = 65_536;
const MAX_PROVISIONAL_BYTES: usize = 65_536;
const MAX_ACTIVITY: usize = 256;
const MAX_TOOL_TRACE_EVENTS: usize = 128;
const OPERATION_POLL_INTERVAL: Duration = Duration::from_millis(40);
const ACTIVE_REFRESH_INTERVAL: Duration = Duration::from_millis(400);
const IDLE_REFRESH_INTERVAL: Duration = Duration::from_secs(2);
const ACTIVE_EVENT_WAIT: Duration = Duration::from_millis(25);
const IDLE_EVENT_WAIT: Duration = Duration::from_millis(250);
// When the Engine advertises durable waits, the active 120-second Turn timeout
// is frozen while approval is pending. This independent, server-enforced
// lifetime lets the TUI opt into worker release. Engines without that optional
// capability receive no durable-wait field at all.
const APPROVAL_WAIT_TTL_MS: u64 = MAX_AGENT_LOOP_WAIT_MS;
const WAIT_CANCEL_COMMAND_DOMAIN: &[u8] = b"y-harness.tui.wait-cancel-command.v1";

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
    Runtime,
    ToolTrace,
}

impl SidebarTab {
    pub(crate) const ALL: [Self; 6] = [
        Self::Activity,
        Self::Sessions,
        Self::Approvals,
        Self::Tasks,
        Self::Runtime,
        Self::ToolTrace,
    ];

    fn next(self) -> Self {
        match self {
            Self::Activity => Self::Sessions,
            Self::Sessions => Self::Approvals,
            Self::Approvals => Self::Tasks,
            Self::Tasks => Self::Runtime,
            Self::Runtime => Self::ToolTrace,
            Self::ToolTrace => Self::Activity,
        }
    }

    fn previous(self) -> Self {
        match self {
            Self::Activity => Self::ToolTrace,
            Self::Sessions => Self::Activity,
            Self::Approvals => Self::Sessions,
            Self::Tasks => Self::Approvals,
            Self::Runtime => Self::Tasks,
            Self::ToolTrace => Self::Runtime,
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct PendingWaitCancel {
    thread_id: ThreadId,
    turn_id: TurnId,
    wait_id: y_harness::AgentLoopWaitId,
    expected_revision: u64,
    command_id: AgentLoopCloseCommandId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PromptSubmissionGate {
    Allowed,
    BlockedNotice(&'static str),
    BlockedError(&'static str),
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
    pub(crate) service_status: Option<ProtocolServiceStatus>,
    pub(crate) runtime_catalog: Option<RuntimeCatalog>,
    pub(crate) tool_trace: VecDeque<ModelStreamEvent>,
    pub(crate) operator_report: Option<Vec<String>>,
    pub(crate) input: String,
    pub(crate) input_cursor: usize,
    pub(crate) transcript_scroll_from_bottom: usize,
    pub(crate) focus: Focus,
    pub(crate) sidebar_tab: SidebarTab,
    pub(crate) active: Option<ActiveTurn>,
    /// Live durable Agent Loop state, independent of a process-local Operation.
    pub(crate) execution: Option<TurnExecutionProjection>,
    /// Last delivery observation returned with an Operation wait settlement.
    pub(crate) approval_delivery: Option<ApprovalDeliveryStatus>,
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
        let (server, capabilities, engine_version) = initialize_engine(client).await?;
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
            service_status: None,
            runtime_catalog: None,
            tool_trace: VecDeque::new(),
            operator_report: None,
            input: String::new(),
            input_cursor: 0,
            transcript_scroll_from_bottom: 0,
            focus: Focus::Composer,
            sidebar_tab: SidebarTab::Activity,
            active: None,
            execution: None,
            approval_delivery: None,
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
                SidebarTab::Activity | SidebarTab::Runtime | SidebarTab::ToolTrace => {}
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
                SidebarTab::Activity | SidebarTab::Runtime | SidebarTab::ToolTrace => {}
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
        match prompt_submission_gate(
            self.execution.as_ref().map(|execution| execution.state),
            self.active.is_some(),
        ) {
            PromptSubmissionGate::Allowed => {}
            PromptSubmissionGate::BlockedNotice(message) => {
                self.set_notice(message);
                return Ok(());
            }
            PromptSubmissionGate::BlockedError(message) => {
                self.set_error(message);
                return Ok(());
            }
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
                approval_wait_ttl_ms: self
                    .capabilities
                    .contains("turn.wait.get")
                    .then_some(APPROVAL_WAIT_TTL_MS),
            })
            .await?;
        let operation_id = match result {
            ProtocolResult::TurnStarted { operation_id } => operation_id,
            result => return Err(unexpected("start Turn", result)),
        };
        self.input.clear();
        self.input_cursor = 0;
        self.execution = None;
        self.approval_delivery = None;
        self.install_active_operation(operation_id);
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
            "/runtime" | "/models" | "/skills" | "/packages" => {
                self.operator_report = None;
                self.sidebar_tab = SidebarTab::Runtime;
                self.focus = Focus::Sidebar;
                self.refresh_runtime(client).await?;
                self.set_notice("Loaded active Runtime catalog");
                Ok(())
            }
            "/trace" | "/tool-trace" => {
                self.operator_report = None;
                self.sidebar_tab = SidebarTab::ToolTrace;
                self.focus = Focus::Sidebar;
                self.refresh_runtime(client).await?;
                self.set_notice("Tool Trace loaded · latest Turn retained locally");
                Ok(())
            }
            "/doctor" => {
                let report = client.diagnose().await?;
                self.operator_report = Some(
                    report
                        .lines()
                        .map(sanitize_operator_line)
                        .collect::<Vec<_>>(),
                );
                self.sidebar_tab = SidebarTab::Runtime;
                self.focus = Focus::Sidebar;
                self.set_notice("Engine doctor report loaded");
                Ok(())
            }
            "/reload" => self.reload_engine(client).await,
            "/refresh" => {
                self.refresh_all(client).await;
                self.set_notice("Protocol projections refreshed");
                Ok(())
            }
            "/cancel" => self.cancel_active(client).await,
            "/resume" => {
                if parts.next().is_some() {
                    return Err(io::Error::other("usage: /resume").into());
                }
                self.resume_wait(client).await
            }
            "/cancelwait" => {
                if parts.next().is_some() {
                    return Err(io::Error::other("usage: /cancelwait").into());
                }
                self.cancel_wait(client).await
            }
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
        self.execution = None;
        self.approval_delivery = None;
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

    async fn resume_wait(&mut self, client: &mut ProtocolClient) -> ClientResult<()> {
        if !self.capabilities.contains("turn.wait.resume") {
            return Err(io::Error::other(
                "Engine does not advertise durable-wait resume capability",
            )
            .into());
        }
        if self.active.is_some() {
            return Err(io::Error::other(
                "a process Operation is already running; use /cancel before another resume",
            )
            .into());
        }
        self.refresh_thread(client).await?;
        self.refresh_execution(client).await?;
        let execution = self
            .execution
            .clone()
            .ok_or_else(|| io::Error::other("current Thread has no live durable wait"))?;
        if execution.state == TurnExecutionState::Executing {
            return Err(io::Error::other(
                "durable execution was already claimed; blind replay is forbidden",
            )
            .into());
        }
        let result = client
            .call(ProtocolCommand::ResumeTurnWait {
                thread_id: execution.thread_id.to_string(),
                turn_id: execution.turn_id.to_string(),
                wait_id: execution.wait_id.to_string(),
                expected_revision: execution.revision,
                memory_scope: MemoryScope::default(),
                context: Vec::new(),
            })
            .await?;
        let operation_id = match result {
            ProtocolResult::TurnStarted { operation_id } => operation_id,
            result => return Err(unexpected("resume durable wait", result)),
        };
        self.install_active_operation(operation_id);
        self.set_notice(format!(
            "Resume requested for wait {} revision {} · verifying durable settlement",
            short_id(execution.wait_id.as_str()),
            execution.revision
        ));
        Ok(())
    }

    async fn cancel_wait(&mut self, client: &mut ProtocolClient) -> ClientResult<()> {
        if !self.capabilities.contains("turn.wait.cancel") {
            return Err(io::Error::other(
                "Engine does not advertise durable-wait cancellation capability",
            )
            .into());
        }
        if self.active.is_some() {
            return Err(io::Error::other(
                "a process Operation is running; use /cancel instead of /cancelwait",
            )
            .into());
        }
        self.refresh_thread(client).await?;
        self.refresh_execution(client).await?;
        let intent = match self.execution.as_ref() {
            Some(execution) if execution.state == TurnExecutionState::Executing => {
                return Err(io::Error::other(
                    "durable execution was already claimed and cannot be closed as an unclaimed wait",
                )
                .into());
            }
            Some(execution) => wait_cancel_intent(execution),
            // A successful close can race with a lost response or process
            // restart. Recover the exact command from authoritative closure
            // evidence and retry it idempotently instead of guessing success.
            None => terminal_wait_cancel_intent(&self.thread)
                .ok_or_else(|| io::Error::other("current Thread has no live durable wait"))?,
        };
        let expected_terminal_revision = intent
            .expected_revision
            .checked_add(1)
            .ok_or_else(|| io::Error::other("wait revision overflow"))?;
        let result = client
            .call(ProtocolCommand::CancelTurnWait {
                thread_id: intent.thread_id.to_string(),
                turn_id: intent.turn_id.to_string(),
                wait_id: intent.wait_id.to_string(),
                expected_revision: intent.expected_revision,
                command_id: intent.command_id.to_string(),
            })
            .await?;
        match result {
            ProtocolResult::TurnWaitCancelled {
                thread_id: settled_thread,
                turn_id,
                wait_id,
                command_id,
                revision,
            } if settled_thread == intent.thread_id
                && turn_id == intent.turn_id
                && wait_id == intent.wait_id
                && command_id == intent.command_id
                && revision == expected_terminal_revision => {}
            result => return Err(unexpected("cancel durable wait", result)),
        }
        self.execution = None;
        self.approval_delivery = None;
        self.refresh_all(client).await;
        self.set_notice(format!(
            "Durable wait {} cancelled at revision {expected_terminal_revision}",
            short_id(intent.wait_id.as_str())
        ));
        Ok(())
    }

    fn install_active_operation(&mut self, operation_id: OperationId) {
        self.transcript_scroll_from_bottom = 0;
        self.tool_trace.clear();
        self.active = Some(ActiveTurn {
            id: operation_id,
            provisional: String::new(),
            provisional_truncated: false,
            stream_gap_through: None,
            started_at: Instant::now(),
            cursor: 0,
        });
        self.last_operation_poll = Instant::now();
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
            {
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
            }
            for event in events {
                let trace = matches!(
                    event.event,
                    ModelStreamEvent::ToolTraceRequest { .. }
                        | ModelStreamEvent::ToolTraceResponse { .. }
                )
                .then(|| event.event.clone());
                let Some(active) = self.active.as_mut() else {
                    return Ok(());
                };
                match &event.event {
                    ModelStreamEvent::TextDelta { delta, .. } => append_provisional(active, delta),
                    ModelStreamEvent::StepInvalidated { .. } => {
                        active.provisional.clear();
                        active.provisional_truncated = false;
                    }
                    ModelStreamEvent::ToolTraceRequest { .. }
                    | ModelStreamEvent::ToolTraceResponse { .. } => {}
                }
                active.cursor = event.sequence;
                if let Some(trace) = trace {
                    self.tool_trace.push_back(trace);
                    while self.tool_trace.len() > MAX_TOOL_TRACE_EVENTS {
                        self.tool_trace.pop_front();
                    }
                }
            }
            if let Some(next) = next
                && let Some(active) = self.active.as_mut()
            {
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
                operation:
                    OperationStatus::Waiting {
                        execution,
                        approval_delivery,
                    },
            } => {
                // Operation completion can arrive before the idle projection
                // refresh. Reload the authoritative Thread before correlating
                // the durable wait so a fast approval boundary is not shown as
                // a transient coordinate error.
                self.refresh_thread(client).await?;
                validate_execution_coordinates(&self.thread, &execution)?;
                let notice = approval_delivery_notice(&execution, &approval_delivery);
                self.execution = Some(execution);
                self.approval_delivery = Some(approval_delivery);
                self.finish_operation(client, &operation_id).await?;
                self.set_notice(notice);
            }
            ProtocolResult::Operation {
                operation: OperationStatus::Completed { .. },
            } => {
                self.finish_operation(client, &operation_id).await?;
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
                self.finish_operation(client, &operation_id).await?;
                self.set_error(error);
            }
            result => return Err(unexpected("poll operation", result)),
        }
        Ok(())
    }

    async fn finish_operation(
        &mut self,
        client: &mut ProtocolClient,
        operation_id: &OperationId,
    ) -> ClientResult<()> {
        match client
            .call(ProtocolCommand::ForgetOperation {
                operation_id: operation_id.to_string(),
            })
            .await?
        {
            ProtocolResult::OperationForgotten {
                operation_id: forgotten,
            } if &forgotten == operation_id => {}
            result => return Err(unexpected("forget terminal operation", result)),
        }
        self.active = None;
        self.refresh_all(client).await;
        self.transcript_scroll_from_bottom = 0;
        if self.quit_after_settlement {
            self.quit = true;
        }
        Ok(())
    }

    async fn refresh_all(&mut self, client: &mut ProtocolClient) {
        let mut errors = Vec::new();
        if let Err(error) = self.refresh_service_status(client).await {
            errors.push(error.to_string());
        }
        match self.refresh_thread(client).await {
            Ok(()) => {
                if let Err(error) = self.refresh_execution(client).await {
                    errors.push(error.to_string());
                }
            }
            Err(error) => {
                errors.push(error.to_string());
            }
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
        if let Err(error) = self.refresh_runtime(client).await {
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

    async fn refresh_execution(&mut self, client: &mut ProtocolClient) -> ClientResult<()> {
        if !self.capabilities.contains("turn.wait.get") {
            self.execution = None;
            self.approval_delivery = None;
            return Ok(());
        }
        let Some(turn_id) = running_turn_id(&self.thread)? else {
            self.execution = None;
            self.approval_delivery = None;
            return Ok(());
        };
        let result = client
            .call(ProtocolCommand::GetTurnExecution {
                thread_id: self.thread.id.to_string(),
                turn_id: turn_id.to_string(),
            })
            .await?;
        let execution = match result {
            ProtocolResult::TurnExecution { execution } => execution,
            result => return Err(unexpected("refresh durable Turn execution", result)),
        };
        if let Some(execution) = execution.as_ref() {
            validate_execution_coordinates(&self.thread, execution)?;
        }
        let same_observation =
            self.execution
                .as_ref()
                .zip(execution.as_ref())
                .is_some_and(|(previous, current)| {
                    previous.wait_id == current.wait_id && previous.revision == current.revision
                });
        if !same_observation {
            self.approval_delivery = None;
        }
        self.execution = execution;
        Ok(())
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

    async fn refresh_runtime(&mut self, client: &mut ProtocolClient) -> ClientResult<()> {
        if !self.capabilities.contains("runtime.catalog") {
            self.runtime_catalog = None;
            return Ok(());
        }
        match client.call(ProtocolCommand::GetRuntimeCatalog {}).await? {
            ProtocolResult::RuntimeCatalog { catalog } => {
                self.runtime_catalog = Some(catalog);
                Ok(())
            }
            result => Err(unexpected("refresh Runtime catalog", result)),
        }
    }

    async fn refresh_service_status(&mut self, client: &mut ProtocolClient) -> ClientResult<()> {
        if !self.capabilities.contains("service.status") {
            self.service_status = None;
            return Ok(());
        }
        match client.call(ProtocolCommand::GetServiceStatus {}).await? {
            ProtocolResult::ServiceStatus { status } => {
                self.service_status = Some(status);
                Ok(())
            }
            result => Err(unexpected("refresh service status", result)),
        }
    }

    async fn reload_engine(&mut self, client: &mut ProtocolClient) -> ClientResult<()> {
        if self.active.is_some() {
            return Err(io::Error::other("cancel the running Turn before reloading").into());
        }
        let thread_id = self.thread.id.to_string();
        client.reload().await?;
        let (server, capabilities, engine_version) = initialize_engine(client).await?;
        self.server = server;
        self.capabilities = capabilities;
        self.engine_version = engine_version;
        self.operator_report = None;
        self.install_thread(load_thread(client, thread_id).await?);
        self.refresh_all(client).await;
        let revision = self
            .runtime_catalog
            .as_ref()
            .map_or("unknown", |catalog| short_id(&catalog.configuration_sha256));
        self.set_notice(format!(
            "Engine generation reloaded at a settled-Turn boundary · config {revision}"
        ));
        Ok(())
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

    /// Returns a terminal status only when the latest Turn crossed a durable
    /// wait boundary. The UI never invents closure from its local wall clock.
    pub(crate) fn latest_wait_terminal_status(&self) -> Option<&TurnStatus> {
        let turn = self.thread.turns.last()?;
        if turn.status == TurnStatus::Running
            || !turn.items.iter().any(|item| {
                matches!(
                    &item.kind,
                    ItemKind::AgentLoopWaitStarted { .. }
                        | ItemKind::AgentLoopResumeAccepted { .. }
                        | ItemKind::AgentLoopReadyClaimed { .. }
                        | ItemKind::AgentLoopWaitClosed { .. }
                        | ItemKind::AgentLoopWaitDenied { .. }
                )
            })
        {
            return None;
        }
        Some(&turn.status)
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
                model_request_sha256: None,
                content: "Keep clients behind Protocol v37.".to_owned(),
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
                "service.status".to_owned(),
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
            service_status: Some(ProtocolServiceStatus {
                admission: ProtocolAdmissionState::Ready,
                running_operations: 0,
                retained_operations: 0,
                operation_retention_limit: 64,
            }),
            runtime_catalog: None,
            tool_trace: VecDeque::new(),
            operator_report: None,
            input: "下一步".to_owned(),
            input_cursor: 3,
            transcript_scroll_from_bottom: 0,
            focus: Focus::Composer,
            sidebar_tab: SidebarTab::Activity,
            active: None,
            execution: None,
            approval_delivery: None,
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

fn sanitize_operator_line(line: &str) -> String {
    line.chars()
        .filter(|character| *character == '\t' || !terminal_unsafe(*character))
        .collect()
}

fn running_turn_id(thread: &Thread) -> Result<Option<TurnId>, io::Error> {
    let mut running = thread
        .turns
        .iter()
        .filter(|turn| turn.status == TurnStatus::Running);
    let turn_id = running.next().map(|turn| turn.id.clone());
    if running.next().is_some() {
        return Err(io::Error::other(
            "authoritative Thread contains multiple running Turns",
        ));
    }
    Ok(turn_id)
}

fn validate_execution_coordinates(
    thread: &Thread,
    execution: &TurnExecutionProjection,
) -> Result<(), io::Error> {
    if execution.thread_id != thread.id
        || running_turn_id(thread)?.as_ref() != Some(&execution.turn_id)
        || execution.revision == 0
    {
        return Err(io::Error::other(
            "Engine returned a durable execution outside the current running Turn",
        ));
    }
    Ok(())
}

fn prompt_submission_gate(
    execution_state: Option<TurnExecutionState>,
    has_attached_operation: bool,
) -> PromptSubmissionGate {
    // An attached Operation owns the only execution path and accepts durable
    // steering. Its briefly stale wait projection must not make the composer
    // advertise a second resume while the claim is already in flight.
    if has_attached_operation {
        return PromptSubmissionGate::Allowed;
    }
    match execution_state {
        Some(TurnExecutionState::Waiting) => PromptSubmissionGate::BlockedNotice(
            "Turn is waiting for an external approval · use /resume or /cancelwait",
        ),
        Some(TurnExecutionState::Ready) => PromptSubmissionGate::BlockedNotice(
            "Approval is ready for exact recovery · use /resume or /cancelwait",
        ),
        Some(TurnExecutionState::Executing) => PromptSubmissionGate::BlockedError(
            "Execution was already claimed by another worker; blind replay is forbidden",
        ),
        None => PromptSubmissionGate::Allowed,
    }
}

/// Derives a restart-stable idempotency identity from typed wait coordinates.
/// Length prefixes and field tags make the preimage unambiguous even when
/// opaque identifiers contain separators.
fn wait_cancel_command_id(
    thread_id: &ThreadId,
    turn_id: &TurnId,
    wait_id: &y_harness::AgentLoopWaitId,
    expected_revision: u64,
) -> AgentLoopCloseCommandId {
    let mut digest = Sha256::new();
    digest.update(WAIT_CANCEL_COMMAND_DOMAIN);
    digest_coordinate(&mut digest, 1, thread_id.as_str().as_bytes());
    digest_coordinate(&mut digest, 2, turn_id.as_str().as_bytes());
    digest_coordinate(&mut digest, 3, wait_id.as_str().as_bytes());
    digest_coordinate(&mut digest, 4, &expected_revision.to_be_bytes());
    let digest = digest.finalize();
    let mut encoded = String::with_capacity(digest.len() * 2);
    const LOWER_HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest {
        encoded.push(char::from(LOWER_HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(LOWER_HEX[usize::from(byte & 0x0f)]));
    }
    AgentLoopCloseCommandId::from_string(format!("tui-wait-cancel-{encoded}"))
}

fn digest_coordinate(digest: &mut Sha256, tag: u8, value: &[u8]) {
    digest.update([tag]);
    digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(value);
}

fn wait_cancel_intent(execution: &TurnExecutionProjection) -> PendingWaitCancel {
    PendingWaitCancel {
        thread_id: execution.thread_id.clone(),
        turn_id: execution.turn_id.clone(),
        wait_id: execution.wait_id.clone(),
        expected_revision: execution.revision,
        command_id: wait_cancel_command_id(
            &execution.thread_id,
            &execution.turn_id,
            &execution.wait_id,
            execution.revision,
        ),
    }
}

/// Recovers only a closure produced by this TUI's deterministic cancellation
/// namespace. Other clients' cancellations and timeout closures remain closed
/// facts and are never replayed under a fabricated command.
fn terminal_wait_cancel_intent(thread: &Thread) -> Option<PendingWaitCancel> {
    let turn = thread.turns.last()?;
    if turn.status != TurnStatus::Cancelled {
        return None;
    }
    turn.items.iter().rev().find_map(|item| {
        let ItemKind::AgentLoopWaitClosed { evidence } = &item.kind else {
            return None;
        };
        let command_id = wait_cancel_command_id(
            &thread.id,
            &turn.id,
            &evidence.wait_id,
            evidence.previous_revision,
        );
        (evidence.command_id == command_id
            && evidence.previous_revision.checked_add(1) == Some(evidence.revision))
        .then(|| PendingWaitCancel {
            thread_id: thread.id.clone(),
            turn_id: turn.id.clone(),
            wait_id: evidence.wait_id.clone(),
            expected_revision: evidence.previous_revision,
            command_id,
        })
    })
}

fn approval_delivery_notice(
    execution: &TurnExecutionProjection,
    delivery: &ApprovalDeliveryStatus,
) -> String {
    let wait = short_id(execution.wait_id.as_str());
    match delivery {
        ApprovalDeliveryStatus::Pending => {
            format!("Turn waiting · approval pending · wait {wait} · /resume after settlement")
        }
        ApprovalDeliveryStatus::Settled => {
            format!("Turn waiting · settlement observed · wait {wait} · use /resume")
        }
        ApprovalDeliveryStatus::Orphaned { reason } => format!(
            "Turn waiting · approval orphaned · wait {wait} · {}",
            clipped_notice(reason, 160)
        ),
        ApprovalDeliveryStatus::Retry { action, message } => format!(
            "Turn waiting · Inbox {action:?} retry required · wait {wait} · {}",
            clipped_notice(message, 160)
        ),
    }
}

fn clipped_notice(value: &str, maximum_chars: usize) -> String {
    let mut characters = value.chars();
    let clipped = characters.by_ref().take(maximum_chars).collect::<String>();
    if characters.next().is_some() {
        format!("{clipped}…")
    } else {
        clipped
    }
}

async fn initialize_engine(
    client: &mut ProtocolClient,
) -> ClientResult<(String, BTreeSet<String>, String)> {
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
    validate_engine_capabilities(&capabilities)?;
    Ok((server, capabilities, engine_version))
}

fn validate_engine_capabilities(capabilities: &BTreeSet<String>) -> ClientResult<()> {
    for required in [
        "operation.cancel",
        "operation.events",
        "operation.forget",
        "operation.get",
        "service.status",
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
    let durable_wait_capabilities = ["turn.wait.cancel", "turn.wait.get", "turn.wait.resume"];
    let durable_wait_count = durable_wait_capabilities
        .iter()
        .filter(|capability| capabilities.contains(**capability))
        .count();
    if durable_wait_count != 0 && durable_wait_count != durable_wait_capabilities.len() {
        return Err(io::Error::other(
            "Engine advertised an incomplete durable-wait capability bundle",
        )
        .into());
    }
    Ok(())
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
        StateEvent::TurnCompleted { turn_id, receipt } => {
            let placement = if receipt.source_thread_id() == &event.thread_id {
                "receipt-bound".to_owned()
            } else {
                format!(
                    "inherited receipt from {}",
                    short_id(receipt.source_thread_id().as_str())
                )
            };
            format!(
                "Turn {} completed · {placement} · candidate {}",
                short_id(turn_id.as_str()),
                short_id(receipt.candidate_item_id().as_str())
            )
        }
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
        StateEvent::WaitStarted { transition, .. } => match &transition.kind {
            ItemKind::AgentLoopWaitStarted { envelope } => format!(
                "Turn waiting · {} · revision {}",
                short_id(envelope.wait_id.as_str()),
                envelope.revision
            ),
            _ => "Turn wait started · invalid transition".to_owned(),
        },
        StateEvent::AcceptResume { transition, .. } => match &transition.kind {
            ItemKind::AgentLoopResumeAccepted { evidence } => format!(
                "Turn ready · {} · revision {}",
                short_id(evidence.wait_id.as_str()),
                evidence.revision
            ),
            _ => "Turn resume accepted · invalid transition".to_owned(),
        },
        StateEvent::ClaimReady { transition, .. } => match &transition.kind {
            ItemKind::AgentLoopReadyClaimed { evidence } => format!(
                "Turn execution claimed · {} · revision {}",
                short_id(evidence.wait_id.as_str()),
                evidence.revision
            ),
            _ => "Turn execution claimed · invalid transition".to_owned(),
        },
        StateEvent::WaitClosed {
            transition, status, ..
        } => match &transition.kind {
            ItemKind::AgentLoopWaitClosed { evidence } => format!(
                "Turn wait closed · {} · {}",
                short_id(evidence.wait_id.as_str()),
                turn_status(status)
            ),
            _ => "Turn wait closed · invalid transition".to_owned(),
        },
        StateEvent::DenyWait { transition, .. } => match &transition.kind {
            ItemKind::AgentLoopWaitDenied { evidence } => format!(
                "Turn wait denied · {} · revision {}",
                short_id(evidence.wait_id.as_str()),
                evidence.revision
            ),
            _ => "Turn wait denied · invalid transition".to_owned(),
        },
        StateEvent::ItemAppended { item, .. } => match &item.kind {
            ItemKind::UserMessage { .. } => "User message".to_owned(),
            ItemKind::ExecutionBinding { binding, .. } => format!(
                "Execution bound · {}@{} · revision {}",
                binding.name(),
                binding.version(),
                binding.revision()
            ),
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
            ItemKind::AgentLoopWaitStarted { envelope } => format!(
                "Turn waiting · {} · revision {}",
                short_id(envelope.wait_id.as_str()),
                envelope.revision
            ),
            ItemKind::AgentLoopResumeAccepted { evidence } => format!(
                "Turn ready · {} · revision {}",
                short_id(evidence.wait_id.as_str()),
                evidence.revision
            ),
            ItemKind::AgentLoopReadyClaimed { evidence } => format!(
                "Turn execution claimed · {} · revision {}",
                short_id(evidence.wait_id.as_str()),
                evidence.revision
            ),
            ItemKind::AgentLoopWaitClosed { evidence } => format!(
                "Turn wait closed · {} · revision {}",
                short_id(evidence.wait_id.as_str()),
                evidence.revision
            ),
            ItemKind::AgentLoopWaitDenied { evidence } => format!(
                "Turn wait denied · {} · revision {}",
                short_id(evidence.wait_id.as_str()),
                evidence.revision
            ),
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
    let suffix = identity.rsplit('-').next().unwrap_or(identity);
    suffix.get(..suffix.len().min(12)).unwrap_or(suffix)
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
    use std::{collections::BTreeSet, time::Instant};

    use y_harness::{
        AgentLoopCloseCommandId, AgentLoopWaitId, Item, ItemKind, OperationId, Thread, Turn,
        TurnExecutionState, TurnStatus, TurnStopReason, WaitClosureEvidence,
    };

    use super::{
        ActiveTurn, MAX_PROVISIONAL_BYTES, PromptSubmissionGate, append_provisional, byte_index,
        prompt_submission_gate, sanitize_operator_line, terminal_wait_cancel_intent,
        validate_engine_capabilities, wait_cancel_command_id,
    };

    fn minimum_capabilities() -> BTreeSet<String> {
        [
            "operation.cancel",
            "operation.events",
            "operation.forget",
            "operation.get",
            "service.status",
            "thread.capacity",
            "thread.create",
            "thread.events",
            "thread.get",
            "thread.name",
            "turn.start",
            "turn.steer",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    }

    #[test]
    fn capability_negotiation_accepts_optional_wait_as_all_or_none() {
        let minimum = minimum_capabilities();
        assert!(validate_engine_capabilities(&minimum).is_ok());

        let mut partial = minimum.clone();
        partial.insert("turn.wait.get".to_owned());
        assert!(validate_engine_capabilities(&partial).is_err());

        partial.insert("turn.wait.cancel".to_owned());
        partial.insert("turn.wait.resume".to_owned());
        assert!(validate_engine_capabilities(&partial).is_ok());

        let mut missing_core = minimum;
        missing_core.remove("turn.start");
        assert!(validate_engine_capabilities(&missing_core).is_err());
    }

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

    #[test]
    fn operator_report_removes_terminal_controls_but_keeps_tabs() {
        assert_eq!(
            sanitize_operator_line("status:\tok\u{1b}[31m"),
            "status:\tok[31m"
        );
    }

    #[test]
    fn durable_execution_gate_never_replays_or_steers_the_wrong_phase() {
        assert!(matches!(
            prompt_submission_gate(Some(TurnExecutionState::Waiting), false),
            PromptSubmissionGate::BlockedNotice(_)
        ));
        assert!(matches!(
            prompt_submission_gate(Some(TurnExecutionState::Ready), false),
            PromptSubmissionGate::BlockedNotice(_)
        ));
        assert!(matches!(
            prompt_submission_gate(Some(TurnExecutionState::Executing), false),
            PromptSubmissionGate::BlockedError(_)
        ));
        assert_eq!(
            prompt_submission_gate(Some(TurnExecutionState::Ready), true),
            PromptSubmissionGate::Allowed
        );
        assert_eq!(
            prompt_submission_gate(Some(TurnExecutionState::Executing), true),
            PromptSubmissionGate::Allowed
        );
        assert_eq!(
            prompt_submission_gate(None, false),
            PromptSubmissionGate::Allowed
        );
    }

    #[test]
    fn wait_cancel_command_identity_is_stable_and_unambiguous() {
        let thread_a = y_harness::ThreadId::from_static("a");
        let thread_ab = y_harness::ThreadId::from_static("ab");
        let turn_bc = y_harness::TurnId::from_static("bc");
        let turn_c = y_harness::TurnId::from_static("c");
        let wait = AgentLoopWaitId::from_static("wait");
        let first = wait_cancel_command_id(&thread_a, &turn_bc, &wait, 7);
        let retry = wait_cancel_command_id(&thread_a, &turn_bc, &wait, 7);
        let ambiguous_without_lengths = wait_cancel_command_id(&thread_ab, &turn_c, &wait, 7);
        let next_revision = wait_cancel_command_id(&thread_a, &turn_bc, &wait, 8);

        assert_eq!(first, retry);
        assert_ne!(first, ambiguous_without_lengths);
        assert_ne!(first, next_revision);
        assert!(first.as_str().starts_with("tui-wait-cancel-"));
        assert_eq!(first.as_str().len(), "tui-wait-cancel-".len() + 64);
    }

    #[test]
    fn terminal_cancel_evidence_restores_exact_retry_after_restart() {
        let mut thread = Thread::new();
        let mut turn = Turn::new(thread.id.clone());
        let wait_id = AgentLoopWaitId::from_static("restart-safe-wait");
        let expected_revision = 2;
        let command_id = wait_cancel_command_id(&thread.id, &turn.id, &wait_id, expected_revision);
        turn.status = TurnStatus::Cancelled;
        turn.items.push(Item::new(ItemKind::AgentLoopWaitClosed {
            evidence: Box::new(WaitClosureEvidence {
                wait_id: wait_id.clone(),
                previous_revision: expected_revision,
                revision: expected_revision + 1,
                command_id: command_id.clone(),
                status: TurnStatus::Cancelled,
                reason: TurnStopReason::Cancelled,
                command_sha256: "0".repeat(64),
                closed_at_ms: 1,
            }),
        }));
        thread.turns.push(turn.clone());

        let recovered = terminal_wait_cancel_intent(&thread).expect("recover exact TUI command");
        assert_eq!(recovered.thread_id, thread.id);
        assert_eq!(recovered.turn_id, turn.id);
        assert_eq!(recovered.wait_id, wait_id);
        assert_eq!(recovered.expected_revision, expected_revision);
        assert_eq!(recovered.command_id, command_id);

        let ItemKind::AgentLoopWaitClosed { evidence } = &mut thread
            .turns
            .last_mut()
            .expect("terminal Turn")
            .items
            .last_mut()
            .expect("closure Item")
            .kind
        else {
            panic!("fixture closure Item changed kind");
        };
        evidence.command_id = AgentLoopCloseCommandId::from_static("foreign-client-command");
        assert!(terminal_wait_cancel_intent(&thread).is_none());
    }
}
