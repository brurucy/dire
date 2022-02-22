use crossterm::event;
use crossterm::event::Event::Key;
use crossterm::event::KeyCode;
use flume::RecvError;
use std::thread;
use std::time::Duration;

pub enum InputEvent {
    Input(KeyCode),
    Tick,
}

pub struct Events {
    sink: flume::Sender<InputEvent>,
    source: flume::Receiver<InputEvent>,
}

impl Events {
    pub fn new(tick_rate: Duration) -> Events {
        let (sink, source) = flume::unbounded();
        let event_sink = sink.clone();
        thread::spawn(move || loop {
            if crossterm::event::poll(tick_rate).unwrap() {
                if let event::Event::Key(key) = event::read().unwrap() {
                    let key = key.code;
                    event_sink.send(InputEvent::Input(key)).unwrap();
                }
            }
            event_sink.send(InputEvent::Tick).unwrap();
        });
        Events { sink, source }
    }
    pub fn next(&self) -> Result<InputEvent, RecvError> {
        self.source.recv()
    }
}
