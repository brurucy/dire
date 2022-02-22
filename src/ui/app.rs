use crate::ui::event::{Events, InputEvent};
use crate::ui::input::InputMode;
use crate::ui::ui::ui;
use crossterm::event;
use crossterm::event::{Event, KeyCode};
use dire_engine::entrypoint::{entrypoint, Engine};
use dire_engine::model::types::Triple;
use dire_parser::load3enc;
use std::borrow::Borrow;
use std::ops::{Add, Deref};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use std::{io, thread};
use tui::backend::Backend;
use tui::Terminal;

#[derive(Clone)]
pub struct App {
    pub input: String,
    pub input_mode: InputMode,
    pub tbox_messages: Vec<((u32, u32, u32), usize, isize)>,
    pub abox_messages: Vec<((u32, u32, u32), usize, isize)>,
}

impl Default for App {
    fn default() -> App {
        App {
            input: String::new(),
            input_mode: InputMode::Normal,
            tbox_messages: Vec::new(),
            abox_messages: Vec::new(),
        }
    }
}

pub fn run_app<B: Backend>(terminal: &mut Terminal<B>, mut app: App) -> io::Result<()> {
    let single_threaded = timely::Config::thread();
    let (
        tbox_input_sink,
        abox_input_sink,
        tbox_output_source,
        abox_output_source,
        termination_source,
    ) = entrypoint(single_threaded, 1, Engine::RDFSpp);

    let (tbox_ui_notifier, tbox_ui_receiver) = flume::unbounded();
    let (abox_ui_notifier, abox_ui_receiver) = flume::unbounded();

    let emitted_triple_output = Arc::new(Mutex::new(0u32));
    let emitted_triple_outputter = emitted_triple_output.clone();

    thread::spawn(move || loop {
        if let Ok(triple) = tbox_output_source.recv_timeout(Duration::from_millis(50)) {
            tbox_ui_notifier.clone().send(triple).unwrap()
        }
    });

    thread::spawn(move || loop {
        if let Ok(triple) = abox_output_source.recv_timeout(Duration::from_millis(50)) {
            *emitted_triple_outputter.lock().unwrap() += 1;
            abox_ui_notifier.send(triple).unwrap()
        }
    });

    let mut tick_counter = 0;
    let event = Events::new(Duration::from_millis(50));
    loop {
        terminal.draw(|f| {
            ui(
                f,
                &app.input_mode,
                &app.input,
                &app.tbox_messages,
                &app.abox_messages,
                &tick_counter,
                &emitted_triple_output.lock().unwrap(),
            )
        })?;
        if let Ok(Event) = event.next() {
            match Event {
                InputEvent::Input(code) => match app.input_mode {
                    InputMode::Normal => match code {
                        KeyCode::Char('t') => app.input_mode = InputMode::TBOX,
                        KeyCode::Char('a') => app.input_mode = InputMode::ABOX,
                        KeyCode::Char('q') => {
                            termination_source.send(()).unwrap();
                            return Ok(());
                        }
                        _ => {}
                    },
                    _ => match code {
                        KeyCode::Enter => {
                            let nt = load3enc(&app.input);
                            match app.input_mode {
                                InputMode::TBOX => {
                                    if let Ok(parsed_nt) = nt {
                                        parsed_nt.for_each(|triple| {
                                            tbox_input_sink.send((triple, 1)).unwrap();
                                        })
                                    }
                                }
                                InputMode::ABOX => {
                                    if let Ok(parsed_nt) = nt {
                                        parsed_nt.for_each(|triple| {
                                            abox_input_sink.send((triple, 1)).unwrap();
                                        })
                                    }
                                }
                                _ => {}
                            }
                            app.input = "".to_string();
                        }
                        KeyCode::Char(c) => {
                            app.input.push(c);
                        }
                        KeyCode::Backspace => {
                            app.input.pop();
                        }
                        KeyCode::Esc => {
                            app.input_mode = InputMode::Normal;
                        }
                        _ => {}
                    },
                },
                InputEvent::Tick => {
                    tick_counter += 1;
                    while let Ok(triple) = tbox_ui_receiver.try_recv() {
                        app.tbox_messages.push(triple)
                    }
                    while let Ok(triple) = abox_ui_receiver.try_recv() {
                        app.abox_messages.push(triple)
                    }
                }
            }
        }
    }
}
