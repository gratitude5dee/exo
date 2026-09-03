use lingua::Message;
use lingua::universal::{AssistantContent, UserContent};
use serde_json::Map;

use crate::agentd::{Run, RunEvent, transcript_rows};

#[test]
fn sse_frame_names_event_and_embeds_discriminator() {
    let frame = RunEvent::MessageDelta {
        delta: "hi".to_string(),
    }
    .to_sse_frame();
    assert_eq!(
        frame,
        "event: message.delta\ndata: {\"event\":\"message.delta\",\"delta\":\"hi\"}\n\n"
    );
}

#[test]
fn tool_started_carries_tool_name_for_airv2_chips() {
    let frame = RunEvent::ToolStarted {
        tool_call_id: "call_1".to_string(),
        tool: "terminal".to_string(),
        arguments: Map::new(),
    }
    .to_sse_frame();
    assert!(frame.starts_with("event: tool.started\n"));
    assert!(frame.contains("\"tool\":\"terminal\""));
}

#[test]
fn terminal_events_serialize_output_and_error() {
    let completed = RunEvent::Completed {
        run_id: "r".to_string(),
        output: "done".to_string(),
    }
    .to_sse_frame();
    assert!(completed.contains("\"event\":\"run.completed\""));
    assert!(completed.contains("\"output\":\"done\""));
    let failed = RunEvent::Failed {
        run_id: "r".to_string(),
        error: "boom".to_string(),
    }
    .to_sse_frame();
    assert!(failed.contains("\"event\":\"run.failed\""));
    assert!(failed.contains("\"error\":\"boom\""));
}

#[test]
fn transcript_rows_keep_only_user_and_assistant_text() {
    let messages = vec![
        Message::System {
            content: UserContent::String("sys".to_string()),
        },
        Message::User {
            content: UserContent::String("hello".to_string()),
        },
        Message::Assistant {
            content: AssistantContent::String("hi there".to_string()),
            id: None,
        },
        Message::Assistant {
            content: AssistantContent::Array(Vec::new()),
            id: None,
        },
    ];
    let rows = serde_json::to_value(transcript_rows(&messages)).expect("rows serialize");
    assert_eq!(
        rows,
        serde_json::json!([
            {"role": "user", "content": "hello"},
            {"role": "assistant", "content": "hi there"}
        ])
    );
}

#[tokio::test]
async fn late_subscribers_replay_then_follow_until_terminal() {
    let run = Run::new("run".to_string(), "session".to_string());
    run.publish(RunEvent::MessageDelta {
        delta: "a".to_string(),
    });
    let mut rx = run.subscribe();
    run.publish(RunEvent::MessageDelta {
        delta: "b".to_string(),
    });
    run.publish(RunEvent::Completed {
        run_id: "run".to_string(),
        output: "ab".to_string(),
    });
    // Nothing after a terminal event is delivered.
    run.publish(RunEvent::MessageDelta {
        delta: "c".to_string(),
    });
    let mut names = Vec::new();
    while let Some(event) = rx.recv().await {
        names.push(event.to_sse_frame().lines().next().unwrap().to_string());
    }
    assert_eq!(
        names,
        vec![
            "event: message.delta",
            "event: message.delta",
            "event: run.completed"
        ]
    );
}

#[tokio::test]
async fn stop_without_task_is_a_no_op() {
    let run = Run::new("run".to_string(), "session".to_string());
    assert!(!run.stop());
    let mut rx = run.subscribe();
    run.publish(RunEvent::Failed {
        run_id: "run".to_string(),
        error: "x".to_string(),
    });
    assert!(rx.recv().await.is_some());
    assert!(rx.recv().await.is_none());
}
