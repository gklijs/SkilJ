//! Entry point: connects read-only to Postgres, loads the bounded
//! context list, and runs a plain poll-driven draw loop - no live
//! subscription, no background task, unlike `skilj-tui`'s own `main.rs`
//! (see `app`'s own doc comment for why that's simpler here, not an
//! oversight).

use clap::Parser;
use crossterm::event::{DisableMouseCapture, EnableMouseCapture, Event, KeyEventKind};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;
use skilj_inspector::app::App;
use skilj_inspector::{cli, data, ui};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = cli::Args::parse();

    // Read-only from here on: `db::connect` alone, never `db::migrate` -
    // see this crate's own root doc comment for why that's a deliberate
    // constraint, not an oversight.
    let pool = skilj_core::db::connect(&args.database_url).await?;
    let bounded_contexts = data::load_bounded_contexts(&pool).await?;
    let mut app = App::new(pool, bounded_contexts);

    let mut terminal = setup_terminal()?;
    let result = run(&mut terminal, &mut app).await;
    restore_terminal(&mut terminal)?;
    result
}

async fn run(
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    app: &mut App,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        terminal.draw(|frame| ui::draw(frame, app))?;
        if crossterm::event::poll(Duration::from_millis(250))? {
            if let Event::Key(key) = crossterm::event::read()? {
                if key.kind == KeyEventKind::Press {
                    app.handle_key(key.code).await;
                }
            }
        }
        if app.should_quit {
            return Ok(());
        }
    }
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
