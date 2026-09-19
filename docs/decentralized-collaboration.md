# Collaboration architecture after legacy cleanup

The `develop-v1.0.0` branch no longer uses `ForumHost` or `forum.log` as an
inter-agent communication channel.

## Runtime communication

- Agent observations and coordination events use Redis Streams.
- Messages are structured `messaging.AgentMessage` objects.
- Agents publish `observation`, `challenge`, `task_offer`, `task_claim`,
  `task_result`, `vote`, and `consensus` events.
- `ForumEngine` is only a legacy log-to-event bridge; it does not call an LLM,
  maintain a host buffer, or write a forum log.
- `agent-event-bridge` is the Docker service that publishes events from the
  three engine log streams to Redis.

## Required environment

```dotenv
MESSAGE_BUS_ENABLED=1
REDIS_URL=redis://redis:6379/0
BETTAFISH_TASK_ID=default
BETTAFISH_ROUND_ID=0
```

`logs/query.log`, `logs/media.log`, and `logs/insight.log` remain ordinary
engine diagnostics. They are not shared conversation state. The shared state
is the Redis Stream `bettafish:{task_id}:events`.

## Migration rule

Do not add new dependencies on `forum.log`, `ForumHost`, or
`get_latest_host_speech`. Use `messaging.RedisStreamBus` or the helpers in
`coordination.agent_events` instead.
