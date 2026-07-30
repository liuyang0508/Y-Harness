//! Full-screen rendering and fail-safe terminal lifecycle.

use std::io;

use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    crossterm::{
        cursor::{Hide, Show},
        event::{DisableBracketedPaste, EnableBracketedPaste},
        execute,
        terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
    },
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, List, ListItem, ListState, Padding, Paragraph, Tabs, Wrap},
};
use unicode_width::UnicodeWidthChar;
use y_harness::{
    ApprovalDecision, ItemKind, MemoryContextRecordStatus, PROTOCOL_VERSION, PolicyDecision,
    RiskLevel, StateCapacity, StateCapacityLevel, TaskStatus, TurnStatus, VerificationOutcome,
};

use crate::app::{App, Focus, SidebarTab};

pub(crate) type Tui = Terminal<CrosstermBackend<io::Stderr>>;

const ACCENT: Color = Color::Rgb(72, 191, 227);
const SURFACE: Color = Color::Rgb(27, 31, 40);
const MUTED: Color = Color::Rgb(125, 133, 148);
const USER: Color = Color::Rgb(130, 170, 255);
const ASSISTANT: Color = Color::Rgb(115, 210, 155);
const WARNING: Color = Color::Rgb(240, 190, 90);
const ERROR: Color = Color::Rgb(240, 100, 105);
const MAX_VISIBLE_ITEMS: usize = 256;
const MAX_RENDERED_TEXT_CHARS: usize = 16_384;
const DEMO_MODEL_ID: &str = "local/demo";

/// Alternate-screen terminal session restored on every ordinary error/unwind.
pub(crate) struct TerminalSession {
    terminal: Tui,
}

impl TerminalSession {
    pub(crate) fn enter() -> io::Result<Self> {
        enable_raw_mode()?;
        let mut stderr = io::stderr();
        if let Err(error) = execute!(stderr, EnterAlternateScreen, EnableBracketedPaste, Hide) {
            let _ = disable_raw_mode();
            return Err(error);
        }
        match Terminal::new(CrosstermBackend::new(stderr)) {
            Ok(terminal) => Ok(Self { terminal }),
            Err(error) => {
                let _ = disable_raw_mode();
                let _ = execute!(
                    io::stderr(),
                    DisableBracketedPaste,
                    LeaveAlternateScreen,
                    Show
                );
                Err(error)
            }
        }
    }

    pub(crate) fn terminal_mut(&mut self) -> &mut Tui {
        &mut self.terminal
    }
}

impl Drop for TerminalSession {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(
            self.terminal.backend_mut(),
            DisableBracketedPaste,
            LeaveAlternateScreen,
            Show
        );
    }
}

pub(crate) fn render(frame: &mut Frame<'_>, app: &App) {
    let area = frame.area();
    if area.width < 60 || area.height < 18 {
        frame.render_widget(
            Paragraph::new(format!(
                "Y-Harness TUI needs at least 60×18 cells.\nCurrent: {}×{}",
                area.width, area.height
            ))
            .alignment(Alignment::Center)
            .block(Block::bordered().title(" terminal too small ")),
            area,
        );
        return;
    }

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(8),
            Constraint::Length(5),
            Constraint::Length(2),
        ])
        .split(area);
    render_header(frame, app, rows[0]);
    render_body(frame, app, rows[1]);
    render_composer(frame, app, rows[2]);
    render_footer(frame, app, rows[3]);
    if app.help {
        render_help(frame, area);
    }
}

fn render_header(frame: &mut Frame<'_>, app: &App, area: Rect) {
    let running = app.active.as_ref().map_or_else(
        || Span::styled("● READY", Style::default().fg(ASSISTANT)),
        |active| {
            let elapsed = active.started_at.elapsed().as_secs_f32();
            Span::styled(
                format!("◐ RUNNING {elapsed:.1}s"),
                Style::default().fg(WARNING).add_modifier(Modifier::BOLD),
            )
        },
    );
    let capacity = app.capacity.map_or_else(
        || ("capacity unavailable".to_owned(), MUTED),
        |capacity| {
            (
                format_capacity(capacity, area.width >= 100),
                capacity_level_color(capacity.level),
            )
        },
    );
    let mut identity_line = vec![
        Span::styled(
            " Y-HARNESS ",
            Style::default()
                .fg(Color::Black)
                .bg(ACCENT)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("  "),
        running,
        Span::styled(
            format!("  {} · protocol v{PROTOCOL_VERSION}", app.server),
            Style::default().fg(MUTED),
        ),
    ];
    if let Some(model_id) = latest_observed_model_id(app) {
        if model_id == DEMO_MODEL_ID {
            identity_line.push(Span::styled(
                " · LAST MODEL local/demo · DETERMINISTIC / NO NETWORK",
                Style::default().fg(WARNING).add_modifier(Modifier::BOLD),
            ));
        } else {
            identity_line.push(Span::styled(
                format!(" · LAST MODEL {}", clipped(model_id, 96)),
                Style::default().fg(ASSISTANT),
            ));
        }
    }
    let lines = vec![
        Line::from(identity_line),
        Line::from(vec![
            Span::styled(
                format!(" engine {} ", app.engine_version),
                Style::default().fg(MUTED),
            ),
            Span::styled(
                format!(
                    "thread {} ",
                    app.thread
                        .name
                        .as_deref()
                        .unwrap_or_else(|| short_id(app.thread.id.as_str()))
                ),
                Style::default().fg(USER),
            ),
            Span::styled(
                app.thread
                    .lineage
                    .as_ref()
                    .map_or_else(String::new, |lineage| {
                        format!("fork of {} · ", short_id(lineage.parent_thread_id.as_str()))
                    }),
                Style::default().fg(MUTED),
            ),
            Span::styled(capacity.0, Style::default().fg(capacity.1)),
        ]),
    ];
    frame.render_widget(
        Paragraph::new(lines)
            .block(Block::default().style(Style::default().bg(SURFACE)))
            .style(Style::default().bg(SURFACE)),
        area,
    );
}

fn render_body(frame: &mut Frame<'_>, app: &App, area: Rect) {
    if area.width >= 100 {
        let columns = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(70), Constraint::Percentage(30)])
            .split(area);
        render_transcript(frame, app, columns[0]);
        render_sidebar(frame, app, columns[1]);
    } else {
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(65), Constraint::Percentage(35)])
            .split(area);
        render_transcript(frame, app, rows[0]);
        render_sidebar(frame, app, rows[1]);
    }
}

fn render_transcript(frame: &mut Frame<'_>, app: &App, area: Rect) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(ACCENT))
        .title(" Conversation · authoritative State ")
        .padding(Padding::horizontal(1));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    if app.thread.turns.is_empty() && app.active.is_none() {
        render_transcript_empty_state(frame, inner);
        return;
    }

    let lines = transcript_lines(app);
    let line_count = wrapped_line_count(&lines, inner.width);
    let paragraph = Paragraph::new(lines).wrap(Wrap { trim: false });
    let max_scroll = line_count.saturating_sub(usize::from(inner.height));
    let from_bottom = app.transcript_scroll_from_bottom.min(max_scroll);
    if max_scroll == 0 {
        let content_height = (line_count.min(usize::from(u16::MAX)) as u16)
            .max(1)
            .min(inner.height);
        let content = Rect {
            y: inner
                .y
                .saturating_add(inner.height.saturating_sub(content_height)),
            height: content_height,
            ..inner
        };
        frame.render_widget(paragraph, content);
        return;
    }
    let scroll = max_scroll
        .saturating_sub(from_bottom)
        .min(usize::from(u16::MAX)) as u16;
    frame.render_widget(paragraph.scroll((scroll, 0)), inner);
}

fn render_transcript_empty_state(frame: &mut Frame<'_>, area: Rect) {
    let lines = vec![
        Line::styled(
            "START A CONVERSATION",
            Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
        ),
        Line::default(),
        Line::styled(
            "The TUI is connected to the headless Engine through Protocol v28.",
            Style::default().fg(Color::White),
        ),
        Line::styled(
            "Type a prompt below and press Enter to create an authoritative Turn.",
            Style::default().fg(Color::White),
        ),
        Line::default(),
        Line::styled(
            "Activity shows durable execution · F1 shows commands and boundaries",
            Style::default().fg(MUTED),
        ),
        Line::styled(
            "The registered Model identity appears after its first durable decision.",
            Style::default().fg(MUTED),
        ),
    ];
    let height = (lines.len().min(usize::from(u16::MAX)) as u16).min(area.height);
    let content = Rect {
        y: area
            .y
            .saturating_add(area.height.saturating_sub(height) / 2),
        height,
        ..area
    };
    frame.render_widget(
        Paragraph::new(lines)
            .alignment(Alignment::Center)
            .wrap(Wrap { trim: false }),
        content,
    );
}

fn transcript_lines(app: &App) -> Vec<Line<'static>> {
    let total_items = app
        .thread
        .turns
        .iter()
        .map(|turn| turn.items.len())
        .sum::<usize>();
    let mut skip = total_items.saturating_sub(MAX_VISIBLE_ITEMS);
    let mut lines = Vec::new();
    if skip > 0 {
        lines.push(Line::styled(
            format!("… {skip} older Items omitted from this viewport"),
            Style::default().fg(MUTED),
        ));
        lines.push(Line::default());
    }
    for turn in &app.thread.turns {
        let visible = turn.items.len().saturating_sub(skip.min(turn.items.len()));
        skip = skip.saturating_sub(turn.items.len());
        if visible == 0 {
            continue;
        }
        lines.push(Line::from(vec![
            Span::styled(
                format!("TURN {} · ", short_id(turn.id.as_str())),
                Style::default().fg(MUTED),
            ),
            Span::styled(
                turn_status(&turn.status),
                Style::default().fg(turn_status_color(&turn.status)),
            ),
            Span::styled(
                format!(" · {} records", turn.items.len()),
                Style::default().fg(MUTED),
            ),
        ]));
        for item in turn.items.iter().skip(turn.items.len() - visible) {
            render_item(&mut lines, &item.kind);
        }
        lines.push(Line::default());
    }
    if let Some(active) = &app.active
        && (!active.provisional.is_empty()
            || active.stream_gap_through.is_some()
            || active.provisional_truncated)
    {
        lines.push(Line::styled(
            "ASSISTANT · provisional",
            Style::default().fg(WARNING).add_modifier(Modifier::BOLD),
        ));
        if let Some(gap) = active.stream_gap_through {
            lines.push(Line::styled(
                format!("stream gap through sequence {gap}"),
                Style::default().fg(ERROR),
            ));
        }
        if active.provisional_truncated {
            lines.push(Line::styled(
                "provisional display truncated at 65,536 bytes; final State remains authoritative",
                Style::default().fg(WARNING),
            ));
        }
        append_multiline(&mut lines, &active.provisional, Color::White);
    }
    lines
}

fn render_item(lines: &mut Vec<Line<'static>>, item: &ItemKind) {
    match item {
        ItemKind::UserMessage { content } => {
            lines.push(Line::styled(
                "YOU",
                Style::default().fg(USER).add_modifier(Modifier::BOLD),
            ));
            append_multiline(lines, content, Color::White);
        }
        ItemKind::ExecutionBinding { binding, .. } => lines.push(Line::styled(
            format!(
                "◇ EXECUTION · {}:{}@{} · revision {}",
                binding.issuer(),
                binding.name(),
                binding.version(),
                binding.revision()
            ),
            Style::default().fg(MUTED),
        )),
        ItemKind::SteeringQueued { steering_id, .. } => {
            lines.push(Line::styled(
                format!("◇ STEERING QUEUED · {}", short_id(steering_id.as_str())),
                Style::default().fg(MUTED),
            ));
        }
        ItemKind::SteeringApplied { content, .. } => {
            lines.push(Line::styled(
                "YOU · STEERING",
                Style::default().fg(USER).add_modifier(Modifier::BOLD),
            ));
            append_multiline(lines, content, Color::White);
        }
        ItemKind::AssistantMessage {
            model_id, content, ..
        } => {
            let model_id = model_id.as_deref().unwrap_or("legacy");
            let mut label = vec![Span::styled(
                format!("ASSISTANT · {model_id}"),
                Style::default().fg(ASSISTANT).add_modifier(Modifier::BOLD),
            )];
            if model_id == DEMO_MODEL_ID {
                label.push(Span::styled(
                    " · DETERMINISTIC DEMO",
                    Style::default().fg(WARNING).add_modifier(Modifier::BOLD),
                ));
            }
            lines.push(Line::from(label));
            append_multiline(lines, content, Color::White);
        }
        ItemKind::ProviderContinuation {
            model_id,
            continuation,
            ..
        } => {
            lines.push(Line::styled(
                format!("◇ PROVIDER STATE · {model_id} · {}", continuation.format()),
                Style::default().fg(MUTED),
            ));
        }
        ItemKind::ToolCall {
            model_id,
            name,
            input,
            ..
        } => {
            lines.push(Line::styled(
                format!(
                    "  ▶ TOOL · {name}{}",
                    model_id.as_deref().map_or_else(String::new, |model_id| {
                        format!(" · requested by {model_id}")
                    })
                ),
                Style::default().fg(WARNING).add_modifier(Modifier::BOLD),
            ));
            lines.push(Line::styled(
                format!("    {}", compact_json(input)),
                Style::default().fg(MUTED),
            ));
        }
        ItemKind::PolicyDecision { decision, .. } => {
            let (label, color) = match decision {
                PolicyDecision::Allow => ("◆ POLICY · allow".to_owned(), ASSISTANT),
                PolicyDecision::Deny { reason } => {
                    (format!("◆ POLICY · deny · {}", clipped(reason, 240)), ERROR)
                }
                PolicyDecision::Ask { reason, risk } => (
                    format!(
                        "◆ POLICY · ask · {} · {}",
                        risk_label(*risk),
                        clipped(reason, 240)
                    ),
                    WARNING,
                ),
            };
            lines.push(Line::styled(
                format!("  {label}"),
                Style::default().fg(color),
            ));
        }
        ItemKind::ApprovalRequested {
            approval_id,
            tool,
            reason,
            risk,
            ..
        } => lines.push(Line::styled(
            format!(
                "⚠ APPROVAL {} · {} · {} · {}",
                short_id(approval_id.as_str()),
                tool,
                risk_label(*risk),
                clipped(reason, 240)
            ),
            Style::default().fg(WARNING),
        )),
        ItemKind::ApprovalDecision { decision, .. } => {
            let (text, color) = match decision {
                ApprovalDecision::Approve => ("approved".to_owned(), ASSISTANT),
                ApprovalDecision::Deny { reason } => {
                    (format!("denied · {}", clipped(reason, 240)), ERROR)
                }
            };
            lines.push(Line::styled(
                format!("⚠ APPROVAL · {text}"),
                Style::default().fg(color),
            ));
        }
        ItemKind::ToolResult {
            output, is_error, ..
        } => {
            lines.push(Line::styled(
                if *is_error {
                    "  ■ TOOL RESULT · failed"
                } else {
                    "  ■ TOOL RESULT · completed"
                },
                Style::default()
                    .fg(if *is_error { ERROR } else { ASSISTANT })
                    .add_modifier(Modifier::BOLD),
            ));
            lines.push(Line::styled(
                format!("    {}", compact_json(output)),
                Style::default().fg(MUTED),
            ));
        }
        ItemKind::RuntimeError { message } => lines.push(Line::styled(
            format!("× RUNTIME · {}", clipped(message, 240)),
            Style::default().fg(ERROR),
        )),
        ItemKind::TurnStopped { reason, phase } => lines.push(Line::styled(
            format!("■ STOPPED · {reason:?} during {phase:?}"),
            Style::default().fg(ERROR),
        )),
        ItemKind::VerificationResult { verifier, outcome } => {
            let (text, color) = match outcome {
                VerificationOutcome::Passed { summary } => (
                    format!(
                        "✓ VERIFY · {verifier}{}",
                        summary.as_deref().map_or_else(String::new, |summary| {
                            format!(" · {}", clipped(summary, 240))
                        })
                    ),
                    ASSISTANT,
                ),
                VerificationOutcome::Failed { reason, retryable } => (
                    format!(
                        "× VERIFY · {verifier} · {} · retryable={retryable}",
                        clipped(reason, 240)
                    ),
                    ERROR,
                ),
            };
            lines.push(Line::styled(text, Style::default().fg(color)));
        }
        ItemKind::MemoryContext {
            provider,
            status,
            packed_tokens,
            ..
        } => lines.push(Line::styled(
            format!(
                "◇ MEMORY · {provider} · {} · {packed_tokens} tokens",
                match status {
                    MemoryContextRecordStatus::Loaded => "loaded",
                    MemoryContextRecordStatus::Degraded => "degraded",
                }
            ),
            Style::default().fg(MUTED),
        )),
        ItemKind::ConversationContext {
            included_turns,
            dropped_turns,
            ..
        } => lines.push(Line::styled(
            format!(
                "◇ CONTEXT · {} Turns · {} omitted",
                included_turns.len(),
                dropped_turns
            ),
            Style::default().fg(MUTED),
        )),
        ItemKind::ConversationSummary {
            compactor,
            covered_turns,
            ..
        } => lines.push(Line::styled(
            format!("◇ SUMMARY · {compactor} · {} Turns", covered_turns.len()),
            Style::default().fg(MUTED),
        )),
        ItemKind::InvocationContext { blocks, .. } => lines.push(Line::styled(
            format!("◇ TURN CONTEXT · {} attributed blocks", blocks.len()),
            Style::default().fg(MUTED),
        )),
    }
}

fn render_sidebar(frame: &mut Frame<'_>, app: &App, area: Rect) {
    let focused = app.focus == Focus::Sidebar;
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(if focused { ACCENT } else { MUTED }))
        .title(" Inspector ");
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Min(1)])
        .split(inner);
    let selected = SidebarTab::ALL
        .iter()
        .position(|tab| *tab == app.sidebar_tab)
        .unwrap_or_default();
    frame.render_widget(
        Tabs::new(["Activity", "Sessions", "Approvals", "Tasks"])
            .select(selected)
            .style(Style::default().fg(MUTED))
            .highlight_style(
                Style::default()
                    .fg(ACCENT)
                    .add_modifier(Modifier::BOLD | Modifier::UNDERLINED),
            )
            .divider("│"),
        rows[0],
    );
    match app.sidebar_tab {
        SidebarTab::Activity => render_activity(frame, app, rows[1]),
        SidebarTab::Sessions => render_sessions(frame, app, rows[1]),
        SidebarTab::Approvals => render_approvals(frame, app, rows[1]),
        SidebarTab::Tasks => render_tasks(frame, app, rows[1]),
    }
}

fn render_activity(frame: &mut Frame<'_>, app: &App, area: Rect) {
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Min(1)])
        .split(area);
    frame.render_widget(
        Paragraph::new(activity_summary(app))
            .style(Style::default().fg(MUTED))
            .wrap(Wrap { trim: false }),
        rows[0],
    );
    let visible = usize::from(rows[1].height.max(1));
    let items = app
        .activity
        .iter()
        .skip(app.activity.len().saturating_sub(visible))
        .map(|entry| {
            ListItem::new(Line::from(vec![
                Span::styled(
                    format!("{:>5} ", entry.sequence),
                    Style::default().fg(MUTED),
                ),
                Span::raw(entry.text.clone()),
            ]))
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        List::new(if items.is_empty() {
            vec![ListItem::new(Line::styled(
                "No durable events",
                Style::default().fg(MUTED),
            ))]
        } else {
            items
        }),
        rows[1],
    );
}

fn activity_summary(app: &App) -> String {
    let thread_events = app.capacity.map_or_else(
        || format!("{} loaded thread events", app.activity.len()),
        |capacity| format!("{} thread events", capacity.used_events),
    );
    match (app.activity.front(), app.activity.back()) {
        (Some(first), Some(last)) if first.sequence == last.sequence => {
            format!("{thread_events} · global sequence {}", first.sequence)
        }
        (Some(first), Some(last)) => format!(
            "{thread_events} · global sequence {}–{}",
            first.sequence, last.sequence
        ),
        _ => format!("{thread_events} · no global sequence loaded"),
    }
}

fn render_sessions(frame: &mut Frame<'_>, app: &App, area: Rect) {
    if !app.capabilities.contains("thread.list") {
        frame.render_widget(
            Paragraph::new("Thread listing is not enabled by this Engine host.")
                .style(Style::default().fg(MUTED))
                .wrap(Wrap { trim: false }),
            area,
        );
        return;
    }
    let mut items = app
        .sessions
        .iter()
        .map(|session| {
            let current = session.thread_id == app.thread.id;
            let mut lines = vec![Line::styled(
                format!(
                    "{}{}",
                    if current { "● " } else { "  " },
                    session
                        .name
                        .as_deref()
                        .unwrap_or_else(|| short_id(session.thread_id.as_str()))
                ),
                Style::default().fg(if current { USER } else { Color::White }),
            )];
            if let Some(lineage) = &session.lineage {
                lines.push(Line::styled(
                    format!(
                        "  ↳ parent {} @ v{}",
                        short_id(lineage.parent_thread_id.as_str()),
                        lineage.parent_stream_version
                    ),
                    Style::default().fg(MUTED),
                ));
            }
            lines.push(Line::styled(
                format!(
                    "{} events · latest sequence {}",
                    session.stream_version, session.last_sequence
                ),
                Style::default().fg(MUTED),
            ));
            ListItem::new(lines)
        })
        .collect::<Vec<_>>();
    if app.sessions_have_more {
        items.push(ListItem::new(Line::styled(
            "… older Threads omitted; use /thread <id>",
            Style::default().fg(MUTED),
        )));
    }
    if items.is_empty() {
        frame.render_widget(
            Paragraph::new("No Threads").style(Style::default().fg(MUTED)),
            area,
        );
        return;
    }
    let mut state = ListState::default();
    state.select(Some(app.selected_session));
    frame.render_stateful_widget(
        List::new(items)
            .highlight_style(Style::default().bg(SURFACE).fg(Color::White))
            .highlight_symbol("▸ "),
        area,
        &mut state,
    );
}

fn render_approvals(frame: &mut Frame<'_>, app: &App, area: Rect) {
    if !app.capabilities.contains("approval.pending") {
        frame.render_widget(
            Paragraph::new("Approval Inbox is not enabled by this Engine host.")
                .style(Style::default().fg(MUTED))
                .wrap(Wrap { trim: false }),
            area,
        );
        return;
    }
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(1), Constraint::Length(3)])
        .split(area);
    let items = app
        .approvals
        .iter()
        .map(|approval| {
            let request = &approval.request;
            ListItem::new(vec![
                Line::styled(
                    format!(
                        "{} · {}",
                        risk_label(request.risk),
                        request.authorization.descriptor.name
                    ),
                    Style::default().fg(WARNING),
                ),
                Line::styled(
                    clipped(&request.reason, 120),
                    Style::default().fg(Color::White),
                ),
            ])
        })
        .collect::<Vec<_>>();
    if items.is_empty() {
        frame.render_widget(
            Paragraph::new("No pending approvals").style(Style::default().fg(MUTED)),
            rows[0],
        );
    } else {
        let mut state = ListState::default();
        state.select(Some(app.selected_approval));
        frame.render_stateful_widget(
            List::new(items)
                .highlight_style(Style::default().bg(SURFACE).fg(Color::White))
                .highlight_symbol("▸ "),
            rows[0],
            &mut state,
        );
    }
    frame.render_widget(
        Paragraph::new(
            "Read-only here: settlement requires a separately authenticated approver principal.",
        )
        .style(Style::default().fg(MUTED))
        .wrap(Wrap { trim: false }),
        rows[1],
    );
}

fn render_tasks(frame: &mut Frame<'_>, app: &App, area: Rect) {
    let Some(graph_id) = app.graph_id.as_deref() else {
        frame.render_widget(
            Paragraph::new("Use /graph <graph-id> to watch a durable Task Graph.")
                .style(Style::default().fg(MUTED))
                .wrap(Wrap { trim: false }),
            area,
        );
        return;
    };
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Min(1)])
        .split(area);
    let summary = app.graph.as_ref().map_or_else(
        || format!("{graph_id} · not found"),
        |graph| {
            format!(
                "{} · rev {} · {} Tasks{}{}",
                graph_id,
                graph.revision,
                graph.task_count,
                if graph.terminal { " · terminal" } else { "" },
                if app.tasks_have_more {
                    " · showing first 64"
                } else {
                    ""
                }
            )
        },
    );
    frame.render_widget(
        Paragraph::new(clipped(&summary, 160)).style(Style::default().fg(ACCENT)),
        rows[0],
    );
    let items = app
        .tasks
        .iter()
        .map(|task| {
            ListItem::new(vec![
                Line::from(vec![
                    Span::styled(
                        task_status_label(&task.status),
                        Style::default().fg(task_status_color(&task.status)),
                    ),
                    Span::raw(format!("  {}", task.definition.id)),
                ]),
                Line::styled(
                    clipped(&task.definition.description, 120),
                    Style::default().fg(MUTED),
                ),
            ])
        })
        .collect::<Vec<_>>();
    if items.is_empty() {
        frame.render_widget(
            Paragraph::new("No Task records").style(Style::default().fg(MUTED)),
            rows[1],
        );
    } else {
        let mut state = ListState::default();
        state.select(Some(app.selected_task));
        frame.render_stateful_widget(
            List::new(items)
                .highlight_style(Style::default().bg(SURFACE))
                .highlight_symbol("▸ "),
            rows[1],
            &mut state,
        );
    }
}

fn render_composer(frame: &mut Frame<'_>, app: &App, area: Rect) {
    let focused = app.focus == Focus::Composer;
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(if focused { ACCENT } else { MUTED }))
        .title(" Composer · Enter send · Ctrl/Alt+Enter newline ");
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let shown = if app.input.is_empty() {
        Paragraph::new("Ask the Harness…  /help for commands")
            .style(Style::default().fg(MUTED))
            .wrap(Wrap { trim: false })
    } else {
        Paragraph::new(app.input.as_str()).wrap(Wrap { trim: false })
    };
    let (row, column) = input_cursor(&app.input, app.input_cursor, inner.width.max(1));
    let scroll = row.saturating_sub(inner.height.saturating_sub(1));
    frame.render_widget(shown.scroll((scroll, 0)), inner);
    if focused && !app.help {
        frame.set_cursor_position((
            inner
                .x
                .saturating_add(column.min(inner.width.saturating_sub(1))),
            inner.y.saturating_add(row.saturating_sub(scroll)),
        ));
    }
}

fn render_footer(frame: &mut Frame<'_>, app: &App, area: Rect) {
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Length(1)])
        .split(area);
    frame.render_widget(
        Paragraph::new(clipped(&app.notice.text, usize::from(area.width)))
            .style(Style::default().fg(if app.notice.error { ERROR } else { MUTED })),
        rows[0],
    );
    frame.render_widget(
        Paragraph::new(
            "Tab focus  ←/→ panel  PgUp/PgDn scroll  Ctrl+N new  Ctrl+R refresh  Esc cancel  F1 help  Ctrl+C exit",
        )
        .style(Style::default().fg(MUTED)),
        rows[1],
    );
}

fn render_help(frame: &mut Frame<'_>, area: Rect) {
    let popup = centered_rect(76, 78, area);
    frame.render_widget(Clear, popup);
    let help = vec![
        Line::styled(
            "Y-Harness TUI",
            Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
        ),
        Line::raw(""),
        Line::raw("Keys"),
        Line::raw("  Enter send                Ctrl/Alt+Enter insert newline"),
        Line::raw("  Tab switch focus          ←/→ switch Inspector panel"),
        Line::raw("  PgUp/PgDn scroll          Ctrl+N create Thread"),
        Line::raw("  Ctrl+R refresh            Esc/Ctrl+C cancel running Turn"),
        Line::raw("  F1 or ? help              Ctrl+C exit when idle"),
        Line::raw(""),
        Line::raw("Commands"),
        Line::raw("  /new                      create and switch Thread"),
        Line::raw("  /fork [terminal-turn-id]  fork history and switch to child"),
        Line::raw("  /name [title]             set or clear Thread name"),
        Line::raw("  /thread <id>              attach to an existing Thread"),
        Line::raw("  /sessions                 list lineage and resume recent Threads"),
        Line::raw("  /graph <id>               watch a durable Task Graph"),
        Line::raw("  /events | /approvals      open Inspector panel"),
        Line::raw("  /refresh | /cancel        refresh or cancel"),
        Line::raw("  /quit                     exit"),
        Line::raw(""),
        Line::styled(
            "Boundary: this client owns only UI state. Engine State, Policy, Tools,",
            Style::default().fg(MUTED),
        ),
        Line::styled(
            "Approvals and Tasks are accessed exclusively through Protocol v28.",
            Style::default().fg(MUTED),
        ),
        Line::raw(""),
        Line::styled("F1 / Esc closes help", Style::default().fg(WARNING)),
    ];
    frame.render_widget(
        Paragraph::new(help).wrap(Wrap { trim: false }).block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(ACCENT))
                .title(" Help ")
                .padding(Padding::uniform(1)),
        ),
        popup,
    );
}

fn append_multiline(lines: &mut Vec<Line<'static>>, text: &str, color: Color) {
    let text = sanitized(text, MAX_RENDERED_TEXT_CHARS, true);
    for line in text.lines() {
        lines.push(Line::styled(line.to_owned(), Style::default().fg(color)));
    }
    if text.is_empty() {
        lines.push(Line::default());
    }
}

fn compact_json(value: &serde_json::Value) -> String {
    serde_json::to_string(value)
        .map(|encoded| clipped(&encoded, 600))
        .unwrap_or_else(|_| "<unrenderable JSON>".to_owned())
}

fn wrapped_line_count(lines: &[Line<'_>], width: u16) -> usize {
    let width = usize::from(width.max(1));
    lines
        .iter()
        .map(|line| line.width().max(1).div_ceil(width))
        .sum()
}

fn clipped(text: &str, maximum_chars: usize) -> String {
    sanitized(text, maximum_chars, false)
}

fn sanitized(text: &str, maximum_chars: usize, preserve_newlines: bool) -> String {
    let mut characters = text.chars();
    let mut safe = String::with_capacity(text.len().min(maximum_chars.saturating_mul(4)));
    for _ in 0..maximum_chars {
        let Some(character) = characters.next() else {
            return safe;
        };
        if character == '\n' && preserve_newlines {
            safe.push(character);
        } else if terminal_unsafe(character) {
            safe.push('�');
        } else {
            safe.push(character);
        }
    }
    if characters.next().is_some() {
        safe.push('…');
    }
    safe
}

pub(crate) fn terminal_unsafe(character: char) -> bool {
    character.is_control()
        || matches!(
            character,
            '\u{061c}'
                | '\u{200e}'
                | '\u{200f}'
                | '\u{202a}'..='\u{202e}'
                | '\u{2066}'..='\u{2069}'
        )
}

fn input_cursor(text: &str, cursor: usize, width: u16) -> (u16, u16) {
    let width = usize::from(width.max(1));
    let mut row = 0_usize;
    let mut column = 0_usize;
    for character in text.chars().take(cursor) {
        if character == '\n' {
            row = row.saturating_add(1);
            column = 0;
            continue;
        }
        let character_width = UnicodeWidthChar::width(character).unwrap_or_default();
        if column.saturating_add(character_width) > width {
            row = row.saturating_add(1);
            column = 0;
        }
        column = column.saturating_add(character_width);
        if column >= width {
            row = row.saturating_add(column / width);
            column %= width;
        }
    }
    (
        row.min(usize::from(u16::MAX)) as u16,
        column.min(usize::from(u16::MAX)) as u16,
    )
}

fn centered_rect(horizontal: u16, vertical: u16, area: Rect) -> Rect {
    let vertical_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - vertical) / 2),
            Constraint::Percentage(vertical),
            Constraint::Percentage((100 - vertical) / 2),
        ])
        .split(area);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - horizontal) / 2),
            Constraint::Percentage(horizontal),
            Constraint::Percentage((100 - horizontal) / 2),
        ])
        .split(vertical_layout[1])[1]
}

fn latest_observed_model_id(app: &App) -> Option<&str> {
    for turn in app.thread.turns.iter().rev() {
        for item in turn.items.iter().rev() {
            match &item.kind {
                ItemKind::AssistantMessage {
                    model_id: Some(model_id),
                    ..
                }
                | ItemKind::ToolCall {
                    model_id: Some(model_id),
                    ..
                }
                | ItemKind::ProviderContinuation { model_id, .. } => return Some(model_id),
                _ => {}
            }
        }
    }
    None
}

fn format_capacity(capacity: StateCapacity, detailed: bool) -> String {
    let event_ratio = ratio_label(capacity.used_events, capacity.event_limit);
    let recovery_ratio = ratio_label(capacity.used_recovery_bytes, capacity.recovery_byte_limit);
    if detailed {
        format!(
            "capacity {} · thread events {}/{} ({event_ratio}) · recovery {}/{} ({recovery_ratio})",
            capacity_level(capacity.level),
            capacity.used_events,
            capacity.event_limit,
            format_bytes(capacity.used_recovery_bytes),
            format_bytes(capacity.recovery_byte_limit),
        )
    } else {
        format!(
            "capacity {} · thread events {event_ratio} · recovery {recovery_ratio}",
            capacity_level(capacity.level)
        )
    }
}

fn ratio_label(used: u64, limit: u64) -> String {
    if limit == 0 {
        return "invalid".to_owned();
    }
    if used == 0 {
        return "0%".to_owned();
    }
    let percentage = (u128::from(used) * 100 / u128::from(limit)).min(100);
    if percentage == 0 {
        "<1%".to_owned()
    } else {
        format!("{percentage}%")
    }
}

fn format_bytes(bytes: u64) -> String {
    for (unit, label) in [
        (1_073_741_824_u64, "GiB"),
        (1_048_576_u64, "MiB"),
        (1_024_u64, "KiB"),
    ] {
        if bytes >= unit {
            let whole = bytes / unit;
            let decimal = bytes % unit * 10 / unit;
            return format!("{whole}.{decimal} {label}");
        }
    }
    format!("{bytes} B")
}

fn capacity_level(level: StateCapacityLevel) -> &'static str {
    match level {
        StateCapacityLevel::Healthy => "healthy",
        StateCapacityLevel::Warning => "warning",
        StateCapacityLevel::Critical => "critical",
        StateCapacityLevel::TerminalOnly => "terminal-only",
        StateCapacityLevel::Exhausted => "exhausted",
    }
}

fn capacity_level_color(level: StateCapacityLevel) -> Color {
    match level {
        StateCapacityLevel::Healthy => ASSISTANT,
        StateCapacityLevel::Warning => WARNING,
        StateCapacityLevel::Critical
        | StateCapacityLevel::TerminalOnly
        | StateCapacityLevel::Exhausted => ERROR,
    }
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

fn turn_status_color(status: &TurnStatus) -> Color {
    match status {
        TurnStatus::Completed => ASSISTANT,
        TurnStatus::Running => WARNING,
        TurnStatus::Failed
        | TurnStatus::Cancelled
        | TurnStatus::TimedOut
        | TurnStatus::Interrupted => ERROR,
    }
}

fn risk_label(risk: RiskLevel) -> &'static str {
    match risk {
        RiskLevel::Low => "low",
        RiskLevel::Medium => "medium",
        RiskLevel::High => "high",
        RiskLevel::Critical => "critical",
    }
}

fn task_status_label(status: &TaskStatus) -> &'static str {
    match status {
        TaskStatus::Pending => "○ pending",
        TaskStatus::Running { .. } => "◐ running",
        TaskStatus::Completed { .. } => "● done",
        TaskStatus::Failed { .. } => "× failed",
        TaskStatus::Cancelled { .. } => "■ cancelled",
        TaskStatus::Blocked { .. } => "◆ blocked",
    }
}

fn task_status_color(status: &TaskStatus) -> Color {
    match status {
        TaskStatus::Pending => MUTED,
        TaskStatus::Running { .. } => WARNING,
        TaskStatus::Completed { .. } => ASSISTANT,
        TaskStatus::Failed { .. } | TaskStatus::Cancelled { .. } | TaskStatus::Blocked { .. } => {
            ERROR
        }
    }
}

fn short_id(identity: &str) -> &str {
    identity.rsplit('-').next().unwrap_or(identity)
}

#[cfg(test)]
mod tests {
    use ratatui::{Terminal, backend::TestBackend};
    use y_harness::{ItemKind, StateCapacity, StateCapacityLevel};

    use crate::app::{ActivityEntry, App, SidebarTab};

    use super::{activity_summary, format_capacity, input_cursor, ratio_label, render, sanitized};

    fn fixture_capacity() -> StateCapacity {
        StateCapacity {
            used_events: 165,
            event_limit: 65_536,
            remaining_events: 65_371,
            general_events_remaining: 65_370,
            terminal_event_reserve: 1,
            used_recovery_bytes: 12_288,
            recovery_byte_limit: 67_108_864,
            remaining_recovery_bytes: 67_096_576,
            general_recovery_bytes_remaining: 67_092_480,
            terminal_recovery_byte_reserve: 4_096,
            level: StateCapacityLevel::Healthy,
        }
    }

    #[test]
    fn composer_cursor_counts_wide_unicode_and_wraps() {
        assert_eq!(input_cursor("ab马", 3, 10), (0, 4));
        assert_eq!(input_cursor("1234马", 5, 5), (1, 2));
        assert_eq!(input_cursor("a\n尾", 3, 10), (1, 2));
    }

    #[test]
    fn untrusted_terminal_text_is_bounded_and_cannot_emit_controls() {
        assert_eq!(
            sanitized("safe\u{1b}[31m\u{202e}txt", 64, false),
            "safe�[31m�txt"
        );
        assert_eq!(sanitized("line\nnext", 64, true), "line\nnext");
        assert_eq!(sanitized("12345", 3, false), "123…");
    }

    #[test]
    fn capacity_usage_never_rounds_nonzero_pressure_to_zero() {
        assert_eq!(ratio_label(0, 65_536), "0%");
        assert_eq!(ratio_label(1, 65_536), "<1%");
        assert_eq!(ratio_label(32_768, 65_536), "50%");
        let rendered = format_capacity(fixture_capacity(), true);
        assert!(rendered.contains("thread events 165/65536 (<1%)"));
        assert!(rendered.contains("recovery 12.0 KiB/64.0 MiB (<1%)"));
        assert!(rendered.contains("capacity healthy"));
    }

    #[test]
    fn activity_distinguishes_thread_count_from_global_sequence()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut app = App::test_fixture()?;
        let mut capacity = fixture_capacity();
        capacity.used_events = 9;
        app.capacity = Some(capacity);
        app.activity.clear();
        app.activity.push_back(ActivityEntry {
            sequence: 166,
            text: "Thread created".to_owned(),
        });
        app.activity.push_back(ActivityEntry {
            sequence: 174,
            text: "Turn completed".to_owned(),
        });
        assert_eq!(
            activity_summary(&app),
            "9 thread events · global sequence 166–174"
        );
        Ok(())
    }

    #[test]
    fn full_screen_renders_engine_projection_and_boundary() -> Result<(), Box<dyn std::error::Error>>
    {
        let backend = TestBackend::new(120, 32);
        let mut terminal = Terminal::new(backend)?;
        let app = App::test_fixture()?;
        terminal.draw(|frame| render(frame, &app))?;
        let screen = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(screen.contains("Y-HARNESS"));
        assert!(screen.contains("Design a safe Harness"));
        assert!(screen.contains("PROVIDER STATE"));
        assert!(screen.contains("fixture.reasoning.v1"));
        assert!(!screen.contains("never-render-this-ciphertext"));
        assert!(screen.contains("STEERING QUEUED"));
        assert!(screen.contains("Prefer the durable boundary"));
        assert!(screen.contains("Keep clients behind Protocol v28"));
        assert!(screen.contains("Harness design"));
        assert!(screen.contains("Activity"));
        Ok(())
    }

    #[test]
    fn short_transcript_stays_near_the_composer() -> Result<(), Box<dyn std::error::Error>> {
        const WIDTH: usize = 120;
        let backend = TestBackend::new(WIDTH as u16, 32);
        let mut terminal = Terminal::new(backend)?;
        let app = App::test_fixture()?;
        terminal.draw(|frame| render(frame, &app))?;
        let turn_row = terminal
            .backend()
            .buffer()
            .content()
            .chunks(WIDTH)
            .position(|row| {
                row.iter()
                    .map(|cell| cell.symbol())
                    .collect::<String>()
                    .contains("TURN")
            })
            .ok_or("rendered transcript has no Turn header")?;
        assert!(
            turn_row > 10,
            "short transcript was not bottom-aligned: row {turn_row}"
        );
        assert!(turn_row < 25, "Turn header overlapped Composer");
        Ok(())
    }

    #[test]
    fn session_panel_renders_authoritative_thread_summaries()
    -> Result<(), Box<dyn std::error::Error>> {
        let backend = TestBackend::new(120, 32);
        let mut terminal = Terminal::new(backend)?;
        let mut app = App::test_fixture()?;
        app.sidebar_tab = SidebarTab::Sessions;
        terminal.draw(|frame| render(frame, &app))?;
        let screen = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(screen.contains("Sessions"));
        assert!(screen.contains("Harness design"));
        assert!(screen.contains("↳ parent parent @ v1"));
        assert!(screen.contains("events · latest sequence"));
        Ok(())
    }

    #[test]
    fn last_durable_demo_decision_is_prominent_without_predicting_the_next_route()
    -> Result<(), Box<dyn std::error::Error>> {
        let backend = TestBackend::new(160, 40);
        let mut terminal = Terminal::new(backend)?;
        let mut app = App::test_fixture()?;
        app.input.clear();
        app.input_cursor = 0;
        let Some(last_item) = app
            .thread
            .turns
            .last_mut()
            .and_then(|turn| turn.items.last_mut())
        else {
            return Err("fixture has no final assistant Item".into());
        };
        let ItemKind::AssistantMessage { model_id, .. } = &mut last_item.kind else {
            return Err("fixture final Item is not an assistant message".into());
        };
        *model_id = Some("local/demo".to_owned());

        terminal.draw(|frame| render(frame, &app))?;
        let screen = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(screen.contains("LAST MODEL local/demo · DETERMINISTIC / NO NETWORK"));
        assert!(screen.contains("ASSISTANT · local/demo · DETERMINISTIC DEMO"));
        assert!(screen.contains("Ask the Harness"));
        assert!(!screen.contains("next model local/demo"));
        Ok(())
    }

    #[test]
    fn empty_thread_explains_the_engine_boundary_and_first_action()
    -> Result<(), Box<dyn std::error::Error>> {
        let backend = TestBackend::new(120, 32);
        let mut terminal = Terminal::new(backend)?;
        let mut app = App::test_fixture()?;
        app.thread.turns.clear();
        app.input.clear();
        app.input_cursor = 0;
        terminal.draw(|frame| render(frame, &app))?;
        let screen = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(screen.contains("START A CONVERSATION"));
        assert!(screen.contains("headless Engine through Protocol v28"));
        assert!(screen.contains("press Enter"));
        assert!(screen.contains("first durable decision"));
        Ok(())
    }

    #[test]
    fn undersized_terminal_renders_a_safe_fallback() -> Result<(), Box<dyn std::error::Error>> {
        let backend = TestBackend::new(50, 12);
        let mut terminal = Terminal::new(backend)?;
        let app = App::test_fixture()?;
        terminal.draw(|frame| render(frame, &app))?;
        let screen = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(screen.contains("needs at least 60×18"));
        Ok(())
    }

    #[test]
    fn minimum_supported_terminal_keeps_all_layouts_bounded()
    -> Result<(), Box<dyn std::error::Error>> {
        let backend = TestBackend::new(60, 18);
        let mut terminal = Terminal::new(backend)?;
        let app = App::test_fixture()?;
        terminal.draw(|frame| render(frame, &app))?;
        let screen = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(screen.contains("Y-HARNESS"));
        assert!(screen.contains("Conversation"));
        assert!(screen.contains("Composer"));
        assert!(!screen.contains("needs at least"));
        Ok(())
    }
}
