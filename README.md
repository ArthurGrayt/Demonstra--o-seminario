# Projeto Kafka com Node.js + TypeScript

Este projeto simula um fluxo de eventos acadêmicos usando Kafka e JavaScript/TypeScript.

## Estrutura

- `academico-producer.ts`: publica eventos no tópico `eventos-academicos`
- `historico-consumer.ts`: consome eventos para atualizar histórico acadêmico
- `notificacao-consumer.ts`: consome eventos para disparar notificações
- `docker-compose.yml`: sobe Kafka e Zookeeper localmente
- `package.json`: scripts para execução

## Requisitos

- Node.js 18+
- npm
- Docker Desktop (ou Docker Engine + Docker Compose)

## 1) Instalar dependências

Na pasta do projeto execute:

```bash
npm install
```

Se ainda não tiver instalado as dependências de desenvolvimento, use:

```bash
npm install -D typescript ts-node @types/node
npm install kafkajs
```

## 2) Subir o Kafka localmente

Na raiz do projeto:

```bash
docker compose up -d
```

Isso sobe:
- Zookeeper
- Kafka em `localhost:9092`

Para verificar se o Kafka subiu corretamente:

```bash
docker compose ps
```

Também pode testar a porta:

```bash
Test-NetConnection -ComputerName localhost -Port 9092
```

No Windows PowerShell, caso a porta ainda não esteja disponível, aguarde alguns segundos e tente novamente.

## 3) Criar o tópico (se necessário)

Como o `KAFKA_AUTO_CREATE_TOPICS_ENABLE` está habilitado no `docker-compose.yml`, normalmente o tópico é criado automaticamente. Porém, se quiser garantir:

```bash
docker exec -it kafka \
  kafka-topics --bootstrap-server localhost:9092 --create --topic eventos-academicos --partitions 1 --replication-factor 1
```

Para listar tópicos:

```bash
docker exec -it kafka \
  kafka-topics --bootstrap-server localhost:9092 --list
```

## 4) Executar os consumers

Abra dois terminais separados na pasta do projeto.

Terminal 1:

```bash
npm run start:historico
```

Terminal 2:

```bash
npm run start:notificacao
```

Esses módulos vão escutar o tópico `eventos-academicos`.

## 5) Executar o producer

Em outro terminal:

```bash
npm run start:producer
```

Isso envia um evento de exemplo com:
- matrícula
- disciplina
- tipo de evento
- valor da nota
- mensagem da notificação

Se tudo estiver funcionando, você verá mensagens como:

- no producer: `Evento de nota publicado no Kafka.`
- no histórico: atualização da média do aluno
- na notificação: disparo da mensagem push

## 6) Scripts disponíveis

No `package.json`, os scripts principais são:

```bash
npm run start:producer
npm run start:historico
npm run start:notificacao
npm run docker:up
docker:down
```

Para derrubar a infraestrutura:

```bash
npm run docker:down
```

## 7) Troubleshooting

### Erro: conexão recusada em `localhost:9092`

Verifique se o Docker está rodando:

```bash
docker ps
```

Se o Kafka não estiver ativo, suba novamente:

```bash
docker compose up -d
```

### Erro: `Cannot find module 'kafkajs'`

Execute:

```bash
npm install
```

### Erro: `ts-node` não encontrado

Execute:

```bash
npm install -D typescript ts-node @types/node
```

## 8) Observações importantes

- O projeto usa `groupId` diferente para cada consumer.
- O `fromBeginning: true` faz com que os consumers leiam mensagens antigas do tópico desde o início.
- O `key` da mensagem usa a matrícula do aluno para manter ordem por aluno.

## 9) Fluxo final esperado

1. Kafka sobe via Docker
2. `historico-consumer` e `notificacao-consumer` entram no grupo e aguardam eventos
3. `academico-producer` publica um evento no tópico `eventos-academicos`
4. Consumers processam a mensagem em paralelo
5. O histórico é atualizado e a notificação é disparada

Se quiser, posso também criar um `tsconfig.json` e um arquivo `.gitignore` para deixar o projeto pronto para uso em equipe.
