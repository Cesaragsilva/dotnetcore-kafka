
using System.Threading.Tasks;
using Transacao.API.Entities;

namespace Transacao.API.Services.Interfaces
{
    public interface IEventos
    {
        Task PublicarEvento(Transaction transacao);
    }
}
