mod ui;

use std::error::Error;
use std::io;
use crossterm::event::{DisableMouseCapture, EnableMouseCapture};
use crossterm::execute;
use crossterm::terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen};
use tui::backend::CrosstermBackend;
use tui::Terminal;
use crate::ui::app::{App, run_app};

fn main() -> Result<(), Box<dyn Error>> {
   // setup terminal
   enable_raw_mode()?;
   let mut stdout = io::stdout();
   execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
   let backend = CrosstermBackend::new(stdout);
   let mut terminal = Terminal::new(backend)?;

   // create app and run it
   let app = App::default();
   let res = run_app(&mut terminal, app);

   // restore terminal
   disable_raw_mode()?;
   execute!(
        terminal.backend_mut(),
        DisableMouseCapture,
       LeaveAlternateScreen
    )?;
   terminal.show_cursor()?;

   if let Err(err) = res {
      println!("{:?}", err)
   }

   Ok(())
}