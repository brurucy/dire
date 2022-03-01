
use crate::ui::input::InputMode;


use tui::backend::Backend;
use tui::layout::{Constraint, Direction, Layout, Rect};
use tui::style::{Color, Modifier, Style};
use tui::text::{Span, Spans, Text};
use tui::widgets::{Block, Borders, List, ListItem, Paragraph};
use tui::Frame;
use unicode_width::UnicodeWidthStr;

fn render_help_text<B: Backend>(f: &mut Frame<B>, input_mode: &InputMode, area: Rect) {
    let (help_msg, style) = match *input_mode {
        InputMode::Normal => (
            vec![
                Span::raw("Press "),
                Span::styled("q", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(" to exit, "),
                Span::styled("t", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(" to insert in the tbox."),
                Span::styled("a", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(" to insert in the abox."),
            ],
            Style::default().add_modifier(Modifier::RAPID_BLINK),
        ),
        _ => (
            vec![
                Span::raw("Press "),
                Span::styled("Esc", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(" to stop editing, "),
                Span::styled("Enter", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(" to load an .ntenc file"),
            ],
            Style::default(),
        ),
    };
    let mut help_text = Text::from(Spans::from(help_msg));
    help_text.patch_style(style);
    let help_text_element = Paragraph::new(help_text);
    f.render_widget(help_text_element, area)
}

fn render_prompt<B: Backend>(f: &mut Frame<B>, input_mode: &InputMode, input: &str, area: Rect) {
    let input = Paragraph::new(input)
        .style(match *input_mode {
            InputMode::Normal => Style::default(),
            _ => Style::default().fg(Color::Yellow),
        })
        .block(Block::default().borders(Borders::ALL).title("Input"));
    f.render_widget(input, area);
}

fn update_cursor<B: Backend>(
    f: &mut Frame<B>,
    input_mode: &InputMode,
    unicode_width: usize,
    area: Rect,
) {
    match *input_mode {
        InputMode::Normal => {}
        _ => f.set_cursor(area.x + unicode_width as u16 + 1, area.y + 1),
    }
}

fn list_messages<B: Backend>(
    f: &mut Frame<B>,
    tbox_messages: Vec<((u32, u32, u32), usize, isize)>,
    area: Rect,
) {
    let items: Vec<ListItem> = tbox_messages
        .iter()
        .map(|each| ListItem::new(format!("{} {} {}", each.0 .0, each.0 .1, each.0 .2)))
        .collect();

    let list = List::new(items).block(Block::default().borders(Borders::ALL).title("Messages"));

    f.render_widget(list, area)
}

fn list_triple_size<B: Backend>(
    f: &mut Frame<B>,
    messages: Vec<((u32, u32, u32), usize, isize)>,
    area: Rect,
) {
    let mut items: Vec<ListItem> = Vec::new();
    items.push(ListItem::new(format!("{:?}", messages.len())));

    let list = List::new(items);

    f.render_widget(list, area)
}

fn list_tick_counter<B: Backend>(f: &mut Frame<B>, tick_counter: &u32, area: Rect) {
    let mut items: Vec<ListItem> = Vec::new();
    items.push(ListItem::new(format!("{}", tick_counter)));

    let list = List::new(items);

    f.render_widget(list, area)
}

pub fn ui<B: Backend>(
    f: &mut Frame<B>,
    input_mode: &InputMode,
    input: &str,
    tbox_messages: &Vec<((u32, u32, u32), usize, isize)>,
    abox_messages: &Vec<((u32, u32, u32), usize, isize)>,
    tick_counter: &u32,
    emitted_triple_output: &u32,
) {
    let frame_size = f.size();

    let header_body_footer = Layout::default()
        .direction(Direction::Vertical)
        .vertical_margin(1)
        .horizontal_margin(2)
        .constraints(
            [
                Constraint::Percentage(10),
                Constraint::Percentage(80),
                Constraint::Percentage(10),
            ]
            .as_ref(),
        )
        .split(frame_size);

    let help_and_input = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(40), Constraint::Percentage(60)].as_ref())
        .split(header_body_footer[0]);

    let abox_tbox_tables = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
        ])
        .split(header_body_footer[1]);

    let abox_tbox_counter = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
        ])
        .split(header_body_footer[2]);

    render_help_text(f, input_mode, help_and_input[0]);
    render_prompt(f, input_mode, input, help_and_input[1]);
    update_cursor(f, input_mode, input.width(), help_and_input[1]);
    list_messages(f, tbox_messages.to_vec(), abox_tbox_tables[0]);
    list_messages(f, abox_messages.to_vec(), abox_tbox_tables[1]);
    list_messages(f, tbox_messages.to_vec(), abox_tbox_tables[2]);
    list_messages(f, abox_messages.to_vec(), abox_tbox_tables[3]);
    list_triple_size(f, tbox_messages.to_vec(), abox_tbox_counter[0]);
    list_triple_size(f, abox_messages.to_vec(), abox_tbox_counter[1]);
    list_tick_counter(f, tick_counter, abox_tbox_counter[2]);
    list_tick_counter(f, emitted_triple_output, abox_tbox_counter[3]);
}
