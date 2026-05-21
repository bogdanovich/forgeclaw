# 🔄 Tarefas Assíncronas e Spawn

> Voltar ao [README](../project/README.pt-br.md)

## Tarefas Rápidas (resposta direta)

- Informar a hora atual

## Tarefas Longas (usar spawn para assíncrono)

- Pesquisar na web notícias sobre IA e resumir
- Verificar e-mail e relatar mensagens importantes
```

**Comportamentos principais:**

| Feature                 | Description                                               |
| ----------------------- | --------------------------------------------------------- |
| **spawn**               | Creates async subagent, doesn't block heartbeat           |
| **Independent context** | Subagent has its own context, no session history          |
| **Delivery mode**       | Completion can be delivered to the user, the parent agent, or both |
| **Non-blocking**        | After spawning, heartbeat continues to next task          |

#### Como Funciona a Comunicação do Subagente

```
Heartbeat é acionado
    ↓
Agente lê HEARTBEAT.md
    ↓
Para tarefa longa: spawn subagente
    ↓                           ↓
Continua para próxima tarefa  Subagente trabalha independentemente
    ↓                           ↓
Todas as tarefas concluídas   Subagente usa ferramenta "message"
    ↓                           ↓
Responde HEARTBEAT_OK         Coordenador de entrega roteia o resultado
```

O subagente tem acesso às ferramentas configuradas, mas a entrega da completion passa pelo fluxo de async task delivery. Use `task_status` para consultar estado durável.

**Configuração:**

```json
{
  "heartbeat": {
    "enabled": true,
    "interval": 30
  }
}
```

| Option     | Default | Description                        |
| ---------- | ------- | ---------------------------------- |
| `enabled`  | `true`  | Enable/disable heartbeat           |
| `interval` | `30`    | Check interval in minutes (min: 5) |

**Variáveis de ambiente:**

* `PICOCLAW_HEARTBEAT_ENABLED=false` para desabilitar
* `PICOCLAW_HEARTBEAT_INTERVAL=60` para alterar o intervalo
