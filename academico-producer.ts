// Importa a biblioteca do cliente Kafka para Node.js
import { Kafka } from 'kafkajs';

// Configura a conexão com o broker local do Kafka
const kafka = new Kafka({
  // Identificador do produtor no cluster
  clientId: 'core-academico',
  // Endereço do broker do Kafka
  brokers: ['localhost:9092'],
});

// Cria a instância do produtor do Kafka
const producer = kafka.producer();

// Função principal assíncrona que publica o evento
async function run() {
  try {
    // Conecta o produtor ao cluster do Kafka
    await producer.connect();
    // Exibe mensagem indicando conexão realizada
    console.log('🚀 [SUAP-Core] Conectado ao broker do Kafka.');

    // Define a matrícula de exemplo para particionamento e identificação
    const matriculaAluno = 'RA-202610';

    // Monta o objeto de evento acadêmico com os dados da avaliação
    const payload = {
      matricula: matriculaAluno,
      disciplina: 'Sistemas Distribuídos',
      tipo: 'NOVA_NOTA',
      valor: 9.5,
      mensagemPush: 'Sua nota de Sistemas Distribuídos foi lançada: 9.5!',
    };

    // Publica a mensagem no tópico Kafka
    await producer.send({
      // Nome do tópico Kafka de destino
      topic: 'eventos-academicos',
      // Lista de mensagens enviadas no lote
      messages: [
        {
          // Chave usada pelo Kafka para rotear para a partição consistente
          key: matriculaAluno,
          // Conteúdo da mensagem convertido em string JSON
          value: JSON.stringify(payload),
        },
      ],
    });

    // Exibe mensagem de confirmação de envio no console
    console.log(`📤 [SUAP-Core] Evento de nota do aluno ${matriculaAluno} publicado com sucesso!`);
  } catch (error) {
    // Exibe detalhes do erro caso ocorra falha na publicação
    console.error('❌ [SUAP-Core] Falha ao publicar evento no Kafka:', error);
  } finally {
    // Garante a desconexão do produtor após o envio para liberar recursos
    await producer.disconnect().catch(() => undefined);
    // Informa que a conexão foi finalizada
    console.log('🔒 [SUAP-Core] Conexão do produtor encerrada.');
  }
}

// Executa a função de envio do evento
run();
