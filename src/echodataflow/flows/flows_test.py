from prefect import flow


@flow(log_prints=True)
def flow_emit_event_ABC(msg: str):
    print(msg)
    print("This flow will emit an event named 'ABC' after it succeeds.")


@flow(log_prints=True)
def flow_triggered_by_event_ABC(msg: str):
    print(msg)
    print("This flow was triggered by event 'ABC'.")
