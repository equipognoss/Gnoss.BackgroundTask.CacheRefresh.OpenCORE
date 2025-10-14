using Es.Riam.AbstractsOpen;
using Es.Riam.Gnoss.AD.EntityModel;
using Es.Riam.Gnoss.AD.EntityModel.Models.CMS;
using Es.Riam.Gnoss.AD.EntityModelBASE;
using Es.Riam.Gnoss.AD.Virtuoso;
using Es.Riam.Gnoss.CL;
using Es.Riam.Gnoss.CL.CMS;
using Es.Riam.Gnoss.CL.ServiciosGenerales;
using Es.Riam.Gnoss.Logica.CMS;
using Es.Riam.Gnoss.Servicios;
using Es.Riam.Gnoss.Util.Configuracion;
using Es.Riam.Gnoss.Util.General;
using Es.Riam.Gnoss.Web.MVC.Models.Administracion;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Net;

namespace Es.Riam.Gnoss.Win.RefrescoCache
{
    class RealizarPeticionWebRefrescoCache : ControladorServicioGnoss
    {
        public CMSComponente FilaComponente { get; set; }
        
        public string UrlRefrescoComponente { get; set; }
        public Dictionary<string, List<string>> PeticionesWebActuales { get; set; }
        private ILogger mlogger;
        private ILoggerFactory mLoggerFactory;
        public RealizarPeticionWebRefrescoCache(IServiceScopeFactory scopedFactory, ConfigService configService, ILogger<RealizarPeticionWebRefrescoCache> logger, ILoggerFactory loggerFactory)
            : base(scopedFactory, configService,logger,loggerFactory)
        {
            mlogger = logger;
            mLoggerFactory = loggerFactory;
        }

        public override void RegistrarInicio(LoggingService loggingService)
        {
        }

        public override void RealizarMantenimiento(EntityContext entityContext, EntityContextBASE entityContextBASE, UtilidadesVirtuoso utilidadesVirtuoso, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, GnossCache gnossCache, VirtuosoAD virtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            DateTime? fechaUltimaActualizacion = null;

            //Actualizo la fecha de la siguiente actualización.
            TipoCaducidadComponenteCMS tipoCaducidad = (TipoCaducidadComponenteCMS)FilaComponente.TipoCaducidadComponente;
            if (ComprobarTipoCaducidadComponenteCaducaPorTiempo(tipoCaducidad))
            {
                DateTime fechaActual = DateTime.Now;
                switch (tipoCaducidad)
                {
                    case TipoCaducidadComponenteCMS.Dia:
                        fechaUltimaActualizacion = fechaActual.AddDays(1);
                        break;
                    case TipoCaducidadComponenteCMS.Hora:
                        fechaUltimaActualizacion = fechaActual.AddHours(1);
                        break;
                    case TipoCaducidadComponenteCMS.Semana:
                        fechaUltimaActualizacion = fechaActual.AddDays(7);
                        break;
                }
            }

            WebClient clienteWeb = null;
            try
            {
                clienteWeb = new WebClient();
                clienteWeb.Headers.Add(HttpRequestHeader.UserAgent, "GnossBotChequeoCache");

                string respuesta = clienteWeb.DownloadString(UrlRefrescoComponente);
                clienteWeb.Dispose();
            }
            catch (Exception ex)
            {
                try
                {
                    loggingService.GuardarLog("Error al refrescar el componente: " + FilaComponente.ComponenteID + " en la url: " + UrlRefrescoComponente + " \r\n" + loggingService.DevolverCadenaError(ex, "1.0"), mlogger);
                }
                catch (Exception) { }

                CMSCL cmsCL = new CMSCL(entityContext, loggingService, redisCacheWrapper, mConfigService, servicesUtilVirtuosoAndReplication, mLoggerFactory.CreateLogger<CMSCL>(), mLoggerFactory);
                cmsCL.InvalidarCacheDeComponentePorIDEnProyectoTodosIdiomas(FilaComponente.ProyectoID, FilaComponente.ComponenteID);
                cmsCL.Dispose();
            }
            finally
            {
                clienteWeb.Dispose();

                string urlHost = new Uri(UrlRefrescoComponente).Host;

                if (PeticionesWebActuales.ContainsKey(urlHost) && PeticionesWebActuales[urlHost].Contains(UrlRefrescoComponente))
                {
                    //Quito la url de las peticiones web actuales
                    PeticionesWebActuales[urlHost].Remove(UrlRefrescoComponente);
                }

                if (fechaUltimaActualizacion.HasValue)
                {
                    ActualizarFechaProximaActualizacionComponente(FilaComponente, fechaUltimaActualizacion.Value, entityContext, loggingService, servicesUtilVirtuosoAndReplication);
                }
            }
        }

        private void ActualizarFechaProximaActualizacionComponente(CMSComponente pFilaComponente, DateTime pFechaActualizacion, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            CMSCN cmsCN = new CMSCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication, mLoggerFactory.CreateLogger<CMSCN>(), mLoggerFactory);
            cmsCN.ActualizarCaducidadComponente(pFilaComponente.ComponenteID, pFechaActualizacion);
            cmsCN.Dispose();
        }

        /// <summary>
        /// Comprueba si un tipo de caducidad es temporal, es decir, es diario, por horas o por semanas
        /// </summary>
        /// <param name="pTipoCaducidad">Tipo de caducidad a comprobar</param>
        /// <returns></returns>
        private bool ComprobarTipoCaducidadComponenteCaducaPorTiempo(TipoCaducidadComponenteCMS pTipoCaducidad)
        {
            return pTipoCaducidad.Equals(TipoCaducidadComponenteCMS.Dia) || pTipoCaducidad.Equals(TipoCaducidadComponenteCMS.Hora) || pTipoCaducidad.Equals(TipoCaducidadComponenteCMS.Semana);
        }

        protected override ControladorServicioGnoss ClonarControlador()
        {
            return new RealizarPeticionWebRefrescoCache(ScopedFactory, mConfigService, mLoggerFactory.CreateLogger<RealizarPeticionWebRefrescoCache>(), mLoggerFactory);
        }
    }
}
