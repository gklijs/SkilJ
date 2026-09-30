//! Entry point: parses `cli::Args`, sets up the terminal, wires the
//! Live Events subscription and the terminal-input reader into one
//! `AppEvent` channel (`app::App::handle` is the only thing that ever
//! mutates state), and runs the draw loop until the user quits.

use clap::Parser;
use crossterm::event::{DisableMouseCapture, EnableMouseCapture};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;
use skilj_tui::app::{App, AppEvent};
use skilj_tui::graphql::{self, Client, TokenSource};
use skilj_tui::{cli, ui};
use std::sync::Arc;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = cli::Args::parse();
    // Before the terminal is taken over, so a failing token command's
    // error reaches the operator as plain output.
    let token = match (&args.token, &args.token_command) {
        (_, Some(command)) => match TokenSource::from_command(command.clone()).await {
            Ok(token) => token,
            Err(e) => {
                eprintln!("skilj-tui: {e}");
                std::process::exit(1);
            }
        },
        (Some(token), None) => TokenSource::fixed(token.clone()),
        (None, None) => unreachable!("clap requires --token or --token-command"),
    };
    let client = Arc::new(Client::with_token_source(
        args.endpoint.clone(),
        token.clone(),
    ));

    let (tx, mut rx) = mpsc::unbounded_channel::<AppEvent>();

    spawn_terminal_input_reader(tx.clone());
    spawn_live_events_subscription(&args, token, tx.clone());

    let mut app = App::new(client, args.bounded_context.clone(), tx);

    let mut terminal = setup_terminal()?;
    let result = run(&mut terminal, &mut app, &mut rx).await;
    restore_terminal(&mut terminal)?;
    result
}

async fn run(
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    app: &mut App,
    rx: &mut mpsc::UnboundedReceiver<AppEvent>,
) -> Result<(), Box<dyn std::error::Error>> {
    terminal.draw(|frame| ui::draw(frame, app))?;
    while let Some(event) = rx.recv().await {
        app.handle(event);
        if app.should_quit {
            break;
        }
        terminal.draw(|frame| ui::draw(frame, app))?;
    }
    Ok(())
}

/// `crossterm::event::read()` blocks the calling thread - run on a
/// dedicated OS thread (not a tokio task: it never yields, so it would
/// otherwise tie up a worker thread) forwarding each event into the same
/// channel everything else feeds.
fn spawn_terminal_input_reader(tx: mpsc::UnboundedSender<AppEvent>) {
    std::thread::spawn(move || loop {
        match crossterm::event::read() {
            Ok(event) => {
                if tx.send(AppEvent::Term(event)).is_err() {
                    return; // the app already quit
                }
            }
            Err(_) => return,
        }
    });
}

fn spawn_live_events_subscription(
    args: &cli::Args,
    token: TokenSource,
    tx: mpsc::UnboundedSender<AppEvent>,
) {
    let ws_endpoint = graphql::to_websocket_url(&args.endpoint);
    // Reconnects, resuming from the last sequence shown, whenever the
    // server ends the subscription (docs/architecture.md §106).
    let mut rx = graphql::spawn_live_events(
        ws_endpoint,
        token,
        args.bounded_context.clone(),
        std::time::Duration::from_millis(500),
        std::time::Duration::from_secs(30),
    );
    tokio::spawn(async move {
        while let Some(result) = rx.recv().await {
            if tx.send(AppEvent::LiveEvent(result)).is_err() {
                return;
            }
        }
    });
}

fn setup_terminal() -> std::io::Result<Terminal<CrosstermBackend<std::io::Stdout>>> {
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    Terminal::new(CrosstermBackend::new(stdout))
}

fn restore_terminal(
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
) -> std::io::Result<()> {
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()
}
