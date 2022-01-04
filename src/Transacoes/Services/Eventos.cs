using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;
using Transacao.API.Entities;
using Transacao.API.Services.Interfaces;

namespace Transacao.API.Services
{
    public class Eventos : IEventos
    {
        private readonly ILogger<Eventos> _logger;
        private readonly ProducerConfig _kafkaConfg;
        private readonly IConfiguration _configuration;
        public Eventos(ILogger<Eventos> logger, ProducerConfig kafkaConfg, IConfiguration configuration)
        {
            _logger = logger;
            _kafkaConfg = kafkaConfg;
            _configuration = configuration;
        }
        public async Task PublicarEvento(Transaction transacao)
        {
            if (transacao == null)
                throw new ArgumentNullException("O Parametro transacao é obrigatório");

            _logger.LogInformation($"Evento recebido para ser publicado {transacao.ObterEventoSerializado()}");

            try
            {
                using (var producer = new ProducerBuilder<Null, string>(_kafkaConfg).Build())
                {
                    var result = await producer.ProduceAsync(_configuration["Kafka:Topicos:Transacoes"], new Message<Null, string>
                    { Value = transacao.ObterEventoSerializado() });

                    _logger.LogInformation($"Evento publicado - Status: {result.Status.ToString()}");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error ao publicar evento: {ex.Message}");
            }
        }
    }
}
