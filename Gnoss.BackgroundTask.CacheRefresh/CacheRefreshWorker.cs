using Es.Riam.Gnoss.Servicios;
using Es.Riam.Gnoss.Util.Configuracion;
using Es.Riam.Gnoss.Win.RefrescoCache;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Gnoss.BackgroundTask.CacheRefresh
{
    public class CacheRefreshWorker : Worker
    {
        private readonly ILogger<CacheRefreshWorker> _logger;
        private readonly ConfigService _configService;

        public CacheRefreshWorker(ILogger<CacheRefreshWorker> logger, ConfigService configService, IServiceScopeFactory scopeFactory) : base(logger, scopeFactory)
        {
            _logger = logger;
            _configService = configService;
        }

        protected override List<ControladorServicioGnoss> ObtenerControladores()
        {
            List<ControladorServicioGnoss> controladores = new List<ControladorServicioGnoss>();
            int numMaxPeticionesWebSimultaneas = _configService.ObtenerNumMaxPeticionesWebSimultaneas();

            controladores.Add(new ControladorRefrescoCache(numMaxPeticionesWebSimultaneas, ScopedFactory, _configService));
            return controladores;
        }
    }
}
