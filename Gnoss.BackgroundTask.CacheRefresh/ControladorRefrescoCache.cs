using System;
using System.Collections.Generic;
using System.Threading;
using System.Data;
using Es.Riam.Gnoss.Util.General;
using Es.Riam.Gnoss.Logica.Live;
using Es.Riam.Gnoss.Recursos;
using Es.Riam.Gnoss.AD.ServiciosGenerales;
using Es.Riam.Gnoss.Logica.ServiciosGenerales;
using Es.Riam.Gnoss.Logica.BASE_BD;
using Es.Riam.Gnoss.AD.BASE_BD.Model;
using Es.Riam.Gnoss.AD.Facetado;
using Es.Riam.Gnoss.Servicios;
using Es.Riam.Gnoss.AD.BASE_BD;
using Es.Riam.Gnoss.Logica.CMS;
using Es.Riam.Gnoss.CL.ServiciosGenerales;
using System.Threading.Tasks;
using Es.Riam.Gnoss.CL.Facetado;
using Es.Riam.Gnoss.AD.Tags;
using Es.Riam.Gnoss.CL.Documentacion;
using Es.Riam.Gnoss.CL.ParametrosProyecto;
using Es.Riam.Gnoss.Servicios.ControladoresServiciosWeb;
using Es.Riam.Gnoss.AD.Parametro;
using Es.Riam.Gnoss.Web.MVC.Models.Administracion;
using Es.Riam.Gnoss.CL.CMS;
using Es.Riam.Gnoss.AD.EntityModel.Models.ParametroGeneralDS;
using System.Linq;
using Es.Riam.Gnoss.Elementos.ParametroGeneralDSEspacio;
using Es.Riam.Gnoss.AD.EncapsuladoDatos;
using Es.Riam.Gnoss.AD.EntityModel.Models.ProyectoDS;
using Es.Riam.Gnoss.AD.EntityModel.Models.CMS;
using Es.Riam.Gnoss.RabbitMQ;
using Es.Riam.Util;
using Newtonsoft.Json;
using Microsoft.Extensions.DependencyInjection;
using Es.Riam.Gnoss.Util.Configuracion;
using Es.Riam.Gnoss.AD.EntityModel;
using Es.Riam.Gnoss.AD.EntityModelBASE;
using Es.Riam.Gnoss.AD.Virtuoso;
using Es.Riam.Gnoss.CL;
using Es.Riam.Gnoss.Elementos.ParametroAplicacion;
using Es.Riam.Gnoss.Web.Controles.ParametroAplicacionGBD;
using Es.Riam.AbstractsOpen;

namespace Es.Riam.Gnoss.Win.RefrescoCache
{
    internal class ControladorRefrescoCache : ControladorServicioGnoss
    {

        #region Constantes

        private const string NOMBRE_COLA = "ColaRefrescoCache";
        private const string EXCHANGE = "";

        #endregion

        #region Miembros

        //private Dictionary<Guid, DateTime> ListaFechasCaducidadComponentes = new Dictionary<Guid, DateTime>();

        //private Dictionary<Guid, CMSDS.CMSComponenteRow> ListaComponentes = new Dictionary<Guid, CMSDS.CMSComponenteRow>();

        //private Dictionary<Guid, List<CMSDS.CMSComponenteRow>> ListaComponentesCaducidadRecursoPorProyecto = new Dictionary<Guid, List<CMSDS.CMSComponenteRow>>();

        //private CMSDS mCmsDS = null;
        /// <summary>
        /// Almacena la lista de peticiones web actuales por dominio
        /// </summary>
        private volatile Dictionary<string, List<string>> mPeticionesWebActuales = new Dictionary<string, List<string>>();

        private Dictionary<CMSComponente, string> mUrlRefrescoComponente = new Dictionary<CMSComponente, string>();

        private Dictionary<Guid, string> mListaUrlPorProyecto = new Dictionary<Guid, string>();

        private List<string> mListaProyectos = new List<string>();

        private int mNumeroMaxPeticionesWebSimultaneas = 5;

        private List<BusquedaRefrescoCaducidad> mListaBusquedaRefrescoCaducidad = new List<BusquedaRefrescoCaducidad>();

        RabbitMQClient mRabbitMQClient;


        #endregion

        #region Constructores

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="pFicheroConfiguracionSitioWeb">Ruta al archivo de configuración del sitio Web</param>
        public ControladorRefrescoCache(int pNumeroMaxPeticionesWebSimultaneas, IServiceScopeFactory serviceScope, ConfigService configService)
            : base(serviceScope, configService)
        {
            mNumeroMaxPeticionesWebSimultaneas = pNumeroMaxPeticionesWebSimultaneas;
        }

        protected override ControladorServicioGnoss ClonarControlador()
        {
            return new ControladorRefrescoCache(mNumeroMaxPeticionesWebSimultaneas, ScopedFactory, mConfigService);
        }

        #endregion

        #region Métodos generales

        Dictionary<string, DateTime> peticionesProcesadas = new Dictionary<string, DateTime>();

        private bool ProcesarItem(string pFila)
        {

            using (var scope = ScopedFactory.CreateScope())
            {
                EntityContext entityContext = scope.ServiceProvider.GetRequiredService<EntityContext>();
                LoggingService loggingService = scope.ServiceProvider.GetRequiredService<LoggingService>();
                VirtuosoAD virtuosoAD = scope.ServiceProvider.GetRequiredService<VirtuosoAD>();
                RedisCacheWrapper redisCacheWrapper = scope.ServiceProvider.GetRequiredService<RedisCacheWrapper>();
                ConfigService configService = scope.ServiceProvider.GetRequiredService<ConfigService>();
                IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication = scope.ServiceProvider.GetRequiredService<IServicesUtilVirtuosoAndReplication>();
                ComprobarTraza("CacheRefresh", entityContext, loggingService, redisCacheWrapper, configService, servicesUtilVirtuosoAndReplication);
                try
                {
                    ComprobarCancelacionHilo();

                    System.Diagnostics.Debug.WriteLine($"ProcesarItem, {pFila}!");

                    if (!string.IsNullOrEmpty(pFila))
                    {
                        object[] itemArray = JsonConvert.DeserializeObject<object[]>(pFila);
                        BaseComunidadDS.ColaRefrescoCacheRow filaCola = (BaseComunidadDS.ColaRefrescoCacheRow)new BaseComunidadDS().ColaRefrescoCache.Rows.Add(itemArray);
                        itemArray = null;

                        string item = $"{filaCola.ProyectoID}{filaCola.TipoBusqueda}{filaCola.TipoEvento}";
                        if (!filaCola.IsInfoExtraNull())
                        {
                            item += filaCola.InfoExtra;
                        }

                        if (peticionesProcesadas.ContainsKey(item) && peticionesProcesadas[item] > filaCola.Fecha)
                        {
                            return true;
                        }
                        else if (!peticionesProcesadas.ContainsKey(item))
                        {
                            peticionesProcesadas.Add(item, DateTime.Now);
                        }
                        else
                        {
                            peticionesProcesadas[item] = DateTime.Now;
                        }

                        if (!filaCola.TipoEvento.Equals((short)TiposEventosRefrescoCache.BusquedaVirtuoso) || (filaCola.TipoEvento.Equals((short)TiposEventosRefrescoCache.BusquedaVirtuoso) && !mListaProyectos.Contains(filaCola.ProyectoID.ToString())))
                        {
                            ProcesarFilaColaRefrescoCache(filaCola, entityContext, loggingService, redisCacheWrapper, virtuosoAD, servicesUtilVirtuosoAndReplication);
                            if (filaCola.TipoEvento.Equals((short)TiposEventosRefrescoCache.BusquedaVirtuoso))
                            {
                                mListaProyectos.Add(filaCola.ProyectoID.ToString());
                            }
                        }

                        filaCola = null;

                        ControladorConexiones.CerrarConexiones(false);
                        UtilPeticion.EliminarObjetosDeHilo(Thread.CurrentThread.ManagedThreadId);
                    }
                    return true;
                }
                catch (Exception ex)
                {
                    loggingService.GuardarLogError(ex);
                    return true;
                }
                finally
                {
                    GuardarTraza(loggingService);
                }
            }
        }

        private void ProcesarFilaColaRefrescoCache(BaseComunidadDS.ColaRefrescoCacheRow pFilaColaRefrescoCache, EntityContext entityContext, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, VirtuosoAD virtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            EstaHiloActivo = true;
            pFilaColaRefrescoCache.Estado = 0;

            if (pFilaColaRefrescoCache.TipoEvento == (short)TiposEventosRefrescoCache.ModificarCaducidadCache)
            {
                try
                {
                    ProcesarFilaModificarCaducidadCache(pFilaColaRefrescoCache, entityContext, loggingService, redisCacheWrapper, servicesUtilVirtuosoAndReplication);
                }
                catch (ThreadAbortException) { }
                catch (Exception ex)
                {
                    pFilaColaRefrescoCache.Estado = 1;
                    EnviarCorreoErrorYGuardarLog(ex, "Error Refresco caché (ProcesarFilaModificarCaducidadCache)", entityContext, loggingService);
                }
            }
            else
            {
                var busquedaRefrescoCaducidad = mListaBusquedaRefrescoCaducidad.FirstOrDefault(item => item.ProyectoID.Equals(pFilaColaRefrescoCache.ProyectoID) && item.TipoBusqueda.Equals(pFilaColaRefrescoCache.TipoBusqueda));
                if (busquedaRefrescoCaducidad == null || busquedaRefrescoCaducidad.Caducidad.AddMinutes(1) < DateTime.Now)
                {
                    if (busquedaRefrescoCaducidad != null)
                    {
                        mListaBusquedaRefrescoCaducidad.Remove(busquedaRefrescoCaducidad);
                    }

                    BusquedaRefrescoCaducidad filaColaRefrescoCache = new BusquedaRefrescoCaducidad();
                    filaColaRefrescoCache.ProyectoID = pFilaColaRefrescoCache.ProyectoID;
                    filaColaRefrescoCache.TipoBusqueda = pFilaColaRefrescoCache.TipoBusqueda;
                    filaColaRefrescoCache.Caducidad = DateTime.Now;
                    mListaBusquedaRefrescoCaducidad.Add(filaColaRefrescoCache);
                }
                else
                {
                    return;
                }

                ComprobarCancelacionHilo();

                ProcesarFila(pFilaColaRefrescoCache, entityContext, loggingService, redisCacheWrapper, virtuosoAD, servicesUtilVirtuosoAndReplication);
            }
        }

        protected void RealizarMantenimientoRabbitMQ(LoggingService loggingService, bool reintentar = true)
        {
            if (mConfigService.ExistRabbitConnection(RabbitMQClient.BD_SERVICIOS_WIN))
            {
                RabbitMQClient.ReceivedDelegate funcionProcesarItem = new RabbitMQClient.ReceivedDelegate(ProcesarItem);
                RabbitMQClient.ShutDownDelegate funcionShutDown = new RabbitMQClient.ShutDownDelegate(OnShutDown);

                mRabbitMQClient = new RabbitMQClient(RabbitMQClient.BD_SERVICIOS_WIN, NOMBRE_COLA, loggingService, mConfigService, EXCHANGE, NOMBRE_COLA);
                mListaProyectos = new List<string>();
                try
                {
                    mRabbitMQClient.ObtenerElementosDeCola(funcionProcesarItem, funcionShutDown);
                    mReiniciarLecturaRabbit = false;
                }
                catch (Exception ex)
                {
                    mReiniciarLecturaRabbit = true;
                    loggingService.GuardarLogError(ex);
                }

            }
        }

        protected void RealizarMantenimientoBaseDatosColas()
        {
            while (true)
            {
                using (var scope = ScopedFactory.CreateScope())
                {
                    EntityContext entityContext = scope.ServiceProvider.GetRequiredService<EntityContext>();
                    EntityContextBASE entityContextBASE = scope.ServiceProvider.GetRequiredService<EntityContextBASE>();
                    UtilidadesVirtuoso utilidadesVirtuoso = scope.ServiceProvider.GetRequiredService<UtilidadesVirtuoso>();
                    LoggingService loggingService = scope.ServiceProvider.GetRequiredService<LoggingService>();
                    VirtuosoAD virtuosoAD = scope.ServiceProvider.GetRequiredService<VirtuosoAD>();
                    RedisCacheWrapper redisCacheWrapper = scope.ServiceProvider.GetRequiredService<RedisCacheWrapper>();
                    GnossCache gnossCache = scope.ServiceProvider.GetRequiredService<GnossCache>();
                    IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication = scope.ServiceProvider.GetRequiredService<IServicesUtilVirtuosoAndReplication>();
                    EstaHiloActivo = true;
                    try
                    {
                        ComprobarCancelacionHilo();

                        if (mReiniciarLecturaRabbit)
                        {
                            RealizarMantenimientoRabbitMQ(loggingService);
                        }

                        BaseComunidadCN baseComunidadCN = new BaseComunidadCN(entityContext, loggingService, entityContextBASE, mConfigService, servicesUtilVirtuosoAndReplication);
                        baseComunidadCN.EliminarColaRefrescoCachePendientesRepetidas();
                        BaseComunidadDS baseComunidadDS = baseComunidadCN.ObtenerColaRefrescoCachePendientes();

                        Dictionary<Guid, List<short>> listaBusquedasRefresco = new Dictionary<Guid, List<short>>();
                        foreach (BaseComunidadDS.ColaRefrescoCacheRow filaCola in baseComunidadDS.ColaRefrescoCache.Rows)
                        {
                            EstaHiloActivo = true;
                            filaCola.Estado = 0;

                            if (filaCola.TipoEvento == (short)TiposEventosRefrescoCache.ModificarCaducidadCache)
                            {
                                try
                                {
                                    ProcesarFilaModificarCaducidadCache(filaCola, entityContext, loggingService, redisCacheWrapper, servicesUtilVirtuosoAndReplication);
                                }
                                catch (ThreadAbortException) { }
                                catch (Exception ex)
                                {
                                    filaCola.Estado = 1;
                                    EnviarCorreoErrorYGuardarLog(ex, "Error Refresco caché (ProcesarFilaModificarCaducidadCache)", entityContext, loggingService);
                                }
                            }
                            else
                            {
                                if (!listaBusquedasRefresco.ContainsKey(filaCola.ProyectoID))
                                {
                                    listaBusquedasRefresco.Add(filaCola.ProyectoID, new List<short>());
                                }
                                if (listaBusquedasRefresco[filaCola.ProyectoID].Contains(filaCola.TipoBusqueda))
                                {
                                    baseComunidadCN.EliminarFilaColaRefrescoCache(filaCola.ColaID);
                                    continue;
                                }
                                else
                                {
                                    listaBusquedasRefresco[filaCola.ProyectoID].Add(filaCola.TipoBusqueda);
                                }

                                ComprobarCancelacionHilo();

                                ProcesarFila(filaCola, entityContext, loggingService, redisCacheWrapper, virtuosoAD, servicesUtilVirtuosoAndReplication);
                            }
                            if (filaCola.Estado == 0)
                            {
                                baseComunidadCN.EliminarFilaColaRefrescoCache(filaCola.ColaID);
                            }
                            else
                            {
                                baseComunidadCN.AcutalizarEstadoColaRefrescoCache(filaCola.ColaID, filaCola.Estado);
                            }
                        }

                        List<CMSComponente> listaComponentesRefresco = CargarComponentesCaducados(entityContext, loggingService, servicesUtilVirtuosoAndReplication);
                        //Esto lo tengo que hacer en un foreach aparte porque si no salta la excepción de Colleción modificada
                        foreach (CMSComponente filacomponente in listaComponentesRefresco)
                        {
                            RefrescarCacheComponente(filacomponente, entityContext, loggingService, redisCacheWrapper, servicesUtilVirtuosoAndReplication);
                        }

                        ComprobarCancelacionHilo();

                        //ControladorConexiones.CerrarConexiones(false);
                    }
                    catch (ThreadAbortException) { }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        loggingService.GuardarLog("ERROR: Excepción: " + ex.ToString() + "\n\n\tTraza: " + ex.StackTrace);
                        ControladorConexiones.CerrarConexiones(false);
                    }
                    finally
                    {
                        //ControladorConexiones.CerrarConexiones(false);
                        //Duermo el proceso el tiempo establecido
                        Thread.Sleep(INTERVALO_SEGUNDOS * 1000);
                    }
                }
            }
            ControladorConexiones.CerrarConexiones(false);
        }

        public override void RealizarMantenimiento(EntityContext entityContext, EntityContextBASE entityContextBASE, UtilidadesVirtuoso utilidadesVirtuoso, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, GnossCache gnossCache, VirtuosoAD virtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            GestorParametroAplicacionDS = new GestorParametroAplicacion();
            ParametroAplicacionGBD parametroAplicacionGBD = new ParametroAplicacionGBD(loggingService, entityContext, mConfigService);
            parametroAplicacionGBD.ObtenerConfiguracionGnoss(GestorParametroAplicacionDS);
            mUrlIntragnoss = GestorParametroAplicacionDS.ParametroAplicacion.Where(parametroApp => parametroApp.Parametro.Equals("UrlIntragnoss")).FirstOrDefault().Valor;

            mDominio = GestorParametroAplicacionDS.ParametroAplicacion.Where(parametroApp => parametroApp.Parametro.Equals("UrlIntragnoss")).FirstOrDefault().Valor;
            mDominio = mDominio.Replace("http://", "").Replace("www.", "");

            if (mDominio[mDominio.Length - 1] == '/')
            {
                mDominio = mDominio.Substring(0, mDominio.Length - 1);
            }
            if (mConfigService.UsarCacheRefreshActiva())
            {
                while (true)
                {
                    Task tarea = Task.Factory.StartNew(() => RealizarMantenimientoRabbitMQ(loggingService));

                    Thread.Sleep(60000); // 1 minuto

                    if (mRabbitMQClient != null)
                    {
                        mRabbitMQClient.CerrarConexionLectura();
                        mRabbitMQClient.Dispose();
                    }

                    Thread.Sleep(300000);// 5 minutos
                }
            }
            else
            {
                RealizarMantenimientoRabbitMQ(loggingService);
            }
        }

        private void ProcesarFila(BaseComunidadDS.ColaRefrescoCacheRow pFilaCola, EntityContext entityContext, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, VirtuosoAD virtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            switch (pFilaCola.TipoEvento)
            {
                case (short)TiposEventosRefrescoCache.BusquedaVirtuoso:
                    try
                    {
                        ProcesarFilaDeBusqueda(pFilaCola, entityContext, loggingService, redisCacheWrapper, virtuosoAD, servicesUtilVirtuosoAndReplication);
                    }
                    catch (ThreadAbortException) { }
                    catch (Exception ex)
                    {
                        pFilaCola.Estado = 1;
                        EnviarCorreoErrorYGuardarLog(ex, "Error Refresco caché (ProcesarFilaDeBusqueda)", entityContext, loggingService);
                    }

                    try
                    {
                        ProcesarFilaDeComponentes(pFilaCola, entityContext, loggingService, redisCacheWrapper, servicesUtilVirtuosoAndReplication);
                    }
                    catch (ThreadAbortException) { }
                    catch (Exception ex)
                    {
                        pFilaCola.Estado = 1;
                        EnviarCorreoErrorYGuardarLog(ex, "Error Refresco caché (ProcesarFilaDeComponentes)", entityContext, loggingService);
                    }
                    break;
                case (short)TiposEventosRefrescoCache.RefrescarComponentesRecursos:
                    try
                    {
                        ProcesarFilaDeComponentes(pFilaCola, entityContext, loggingService, redisCacheWrapper, servicesUtilVirtuosoAndReplication);
                    }
                    catch (ThreadAbortException) { }
                    catch (Exception ex)
                    {
                        pFilaCola.Estado = 1;
                        EnviarCorreoErrorYGuardarLog(ex, "Error Refresco caché (ProcesarFilaDeComponentes)", entityContext, loggingService);
                    }
                    break;
                case (short)TiposEventosRefrescoCache.ConfiguracionComponentesCambiada:
                    try
                    {
                        if (!pFilaCola.IsInfoExtraNull())
                        {
                            Guid idComponente = new Guid(pFilaCola.InfoExtra);

                            RefrescarCacheComponente(idComponente, pFilaCola.ProyectoID, entityContext, loggingService, redisCacheWrapper, servicesUtilVirtuosoAndReplication);
                        }
                    }
                    catch (ThreadAbortException) { }
                    catch (Exception ex)
                    {
                        pFilaCola.Estado = 1;
                        EnviarCorreoErrorYGuardarLog(ex, "Error Refresco caché (ProcesarFilaDeComponentes)", entityContext, loggingService);
                    }
                    break;
            }
        }

        private void ProcesarFilaModificarCaducidadCache(BaseComunidadDS.ColaRefrescoCacheRow pFilaCola, EntityContext entityContext, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            DocumentacionCL docCL = new DocumentacionCL(mFicheroConfiguracionBD, mFicheroConfiguracionBDRecursos, entityContext, loggingService, redisCacheWrapper, mConfigService, servicesUtilVirtuosoAndReplication);
            docCL.Dominio = mDominio;
            docCL.ActualizarCaducidadControlesCache(pFilaCola.ProyectoID, new Guid(pFilaCola.InfoExtra));
        }

        private void ProcesarFilaDeComponentes(BaseComunidadDS.ColaRefrescoCacheRow pFilaCola, EntityContext entityContext, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            List<CMSComponente> listaComponentes = CargarComponentesProyectoCaducidadRecurso(pFilaCola.ProyectoID, entityContext, loggingService, servicesUtilVirtuosoAndReplication);

            //Si BusquedaVirtuoso o RefrescarComponentesRecursos, buscar todos los componentes que tienen caducidad recurso y refrescarlos
            if (listaComponentes.Count > 0)
            {
                foreach (CMSComponente filaComponente in listaComponentes)
                {
                    RefrescarCacheComponente(filaComponente, entityContext, loggingService, redisCacheWrapper, servicesUtilVirtuosoAndReplication);
                }
            }
        }

        /// <summary>
        /// Actualizamos el contador de num elementos nuevos para cada perfil.
        /// </summary>
        /// <param name="pPerfiles">Lista de perfiles que han recibido un correo.</param>
        private void AgregarNotificacionCorreoNuevoAIdentidades(List<Guid> pPerfiles, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            LiveCN liveCN = new LiveCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            foreach (Guid perfilID in pPerfiles)
            {
                liveCN.AumentarContadorNuevosMensajes(perfilID);
            }
            liveCN.Dispose();
        }

        /// <summary>
        /// Comprueba si un tag proviene de un filtro
        /// </summary>
        /// <param name="pTags">Cadena que contiene los tags</param>
        /// <param name="pListaTagsFiltros">Lista de tags que provienen de filtros</param>
        /// <param name="pListaTodosTags">Lista de todos los tags</param>
        /// <param name="pDataSet">Data set de la fila de cola</param>
        /// <returns></returns>
        private Dictionary<short, List<string>> ObtenerTagsFiltros(string pTags)
        {
            Dictionary<short, List<string>> listaTagsFiltros = new Dictionary<short, List<string>>();
            //MensajeID
            listaTagsFiltros.Add((short)TiposTags.IDTagMensaje, BuscarTagFiltroEnCadena(ref pTags, Constantes.ID_MENSAJE));

            //Identidad que envia el mensaje
            listaTagsFiltros.Add((short)TiposTags.IDTagMensajeFrom, BuscarTagFiltroEnCadena(ref pTags, Constantes.ID_MENSAJE_FROM));

            //Identidad que recibe el mensaje
            listaTagsFiltros.Add((short)TiposTags.IDTagMensajeTo, BuscarTagFiltroEnCadena(ref pTags, Constantes.IDS_MENSAJE_TO));
            return listaTagsFiltros;
        }

        /// <summary>
        /// Busca un filtro concreto en una cadena
        /// </summary>
        /// <param name="pCadena">Cadena en la que se debe buscar</param>
        /// <param name="pClaveFiltro">Clave del filtro (##CAT_DOC##, ...)</param>
        /// <returns></returns>
        private List<string> BuscarTagFiltroEnCadena(ref string pCadena, string pClaveFiltro)
        {
            string filtro = "";
            List<string> listaFiltros = new List<string>();

            int indiceFiltro = pCadena.IndexOf(pClaveFiltro);

            if (indiceFiltro >= 0)
            {
                string subCadena = pCadena.Substring(indiceFiltro + pClaveFiltro.Length);

                filtro = subCadena.Substring(0, subCadena.IndexOf(pClaveFiltro));

                if ((pClaveFiltro.Equals(Constantes.TIPO_DOC)) || (pClaveFiltro.Equals(Constantes.PERS_U_ORG)) || (pClaveFiltro.Equals(Constantes.ESTADO_COMENTADO)))
                {
                    //Estos tags van con la clave del tag (para tags de tipo entero o similar, ej: Tipos de documento, para que al buscar '0' no aparezcan los tags de todos los recursos que son de tal tipo). 
                    filtro = pClaveFiltro + filtro + pClaveFiltro;
                    pCadena = pCadena.Replace(filtro, "");
                }
                else
                {
                    pCadena = pCadena.Replace(pClaveFiltro + filtro + pClaveFiltro, "");
                    filtro = filtro.ToLower();
                }
                if (filtro.Trim() != "")
                {
                    listaFiltros.Add(filtro);
                }
                listaFiltros.AddRange(BuscarTagFiltroEnCadena(ref pCadena, pClaveFiltro));
            }
            return listaFiltros;
        }

        private void ProcesarFilaDeBusqueda(BaseComunidadDS.ColaRefrescoCacheRow pFilaCola, EntityContext entityContext, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, VirtuosoAD virtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            // Cargar los datos del proyecto
            string urlPropiaProyecto = string.Empty;
            ProyectoCN proyectoCN = new ProyectoCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            urlPropiaProyecto = proyectoCN.ObtenerURLPropiaProyecto(pFilaCola.ProyectoID);
            proyectoCN.Dispose();

            CargadorResultados cargadorResultados = new CargadorResultados();
            cargadorResultados.Url = mConfigService.ObtenerUrlServicioResultados();

            CargadorFacetas cargadorFacetas = new CargadorFacetas();
            cargadorFacetas.Url = mConfigService.ObtenerUrlServicioFacetas();

            string ubicacionBusqueda = "Particular";

            if (pFilaCola.TipoBusqueda.Equals(TipoBusqueda.PersonasYOrganizaciones))
            {
                ubicacionBusqueda = "PersonaInicial";
            }

            string parametros_adiccionales = "";
            if (!pFilaCola.IsInfoExtraNull())
            {
                parametros_adiccionales = pFilaCola.InfoExtra;
                ubicacionBusqueda = "Meta";
            }

            //Comprobar que en el proyecto se permite pintar la pestanya de recursos
            ParametroGeneral filaParametroGeneral = ObtenerFilaParametroGeneral(pFilaCola.ProyectoID, entityContext, loggingService, redisCacheWrapper, servicesUtilVirtuosoAndReplication);

            if (filaParametroGeneral != null && filaParametroGeneral.PestanyaRecursosVisible)
            {
                ProyectoCL proyectoCL = new ProyectoCL(entityContext, loggingService, redisCacheWrapper, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                Dictionary<string, string> parametroProyecto = proyectoCL.ObtenerParametrosProyecto(pFilaCola.ProyectoID);
                proyectoCL.Dispose();

                // Obtener idiomas del ecosistema y reprocesar teniendo en cuenta los filtros.
                Dictionary<string, string> listaIdiomas = mConfigService.ObtenerListaIdiomasDictionary();
                if (!parametroProyecto.ContainsKey(ParametroAD.PropiedadContenidoMultiIdioma))
                {
                    // Por cada idioma recogeneramos la caché de usuarios conectados, invitados y bots.
                    foreach (string idioma in listaIdiomas.Keys)
                    {
                        RegenerarCacheBusquedaUsuariosYBots(cargadorResultados, cargadorFacetas, pFilaCola, ubicacionBusqueda, idioma, parametros_adiccionales, loggingService);
                    }
                }
                else
                {
                    // Por cada idioma recogeneramos la caché de usuarios conectados, invitados y bots.
                    foreach (string idioma in listaIdiomas.Keys)
                    {
                        string parametrosAdiccionalesTemporales = parametros_adiccionales + "|" + parametroProyecto[ParametroAD.PropiedadContenidoMultiIdioma] + "=" + idioma;

                        RegenerarCacheBusquedaUsuariosYBots(cargadorResultados, cargadorFacetas, pFilaCola, ubicacionBusqueda, idioma, parametrosAdiccionalesTemporales, loggingService);
                    }
                }
            }

            if (!pFilaCola.TipoBusqueda.Equals((short)TipoBusqueda.PersonasYOrganizaciones))
            {
                FacetadoCL facetadoCL = new FacetadoCL(mUrlIntragnoss, entityContext, loggingService, redisCacheWrapper, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                facetadoCL.Dominio = mDominio;
                facetadoCL.InvalidarResultadosYFacetasDeBusquedaEnProyecto(pFilaCola.ProyectoID, FacetadoAD.TipoBusquedaToString((TipoBusqueda)pFilaCola.TipoBusqueda), true);
                facetadoCL.BorrarRSSDeComunidad(pFilaCola.ProyectoID);

                /*if (!pFilaCola.IsInfoExtraNull() && pFilaCola.InfoExtra.StartsWith("rdf:type="))
                {
                    facetadoCL.InvalidarResultadosYFacetasDeBusquedaEnProyecto(pFilaCola.ProyectoID, pFilaCola.InfoExtra, true);
                }*/

                //Si contiene el tipo de búsqueda que se está pidiendo se necesitan limpiar todas las pestanyas que contengan esa búsqueda.
                ProyectoCN proyCN = new ProyectoCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                DataWrapperProyecto dataWrapperProyecto = proyCN.ObtenerProyectoPorID(pFilaCola.ProyectoID);
                foreach (ProyectoPestanyaBusqueda pestanyas in dataWrapperProyecto.ListaProyectoPestanyaBusqueda)
                {
                    if (pestanyas.CampoFiltro.Contains(FacetadoAD.TipoBusquedaToString((TipoBusqueda)pFilaCola.TipoBusqueda)) || (!pFilaCola.IsInfoExtraNull() && pestanyas.CampoFiltro.Contains(pFilaCola.InfoExtra)))
                    {
                        //Pasamos el parámetro UsuariosConPrivados = false para que limpie las claves de caché.
                        facetadoCL.InvalidarResultadosYFacetasDeBusquedaEnProyecto(pFilaCola.ProyectoID, pestanyas.CampoFiltro, false);
                    }
                }

                proyCN.Dispose();

                facetadoCL.Dispose();
            }
        }

        private void RegenerarCacheBusquedaUsuariosYBots(CargadorResultados pCargadorResultados, CargadorFacetas pCargadorFacetas, BaseComunidadDS.ColaRefrescoCacheRow pFilaCola, string pUbicacionBusqueda, string pIdioma, string pParametros_Adiccionales, LoggingService loggingService)
        {
            #region Usuario Conectado

            //Usuario conectado                
            try
            {
                // resultados
                pCargadorResultados.RefrescarResultados(pFilaCola.ProyectoID, pFilaCola.ProyectoID.Equals(ProyectoAD.MetaProyecto), true, false, true, pIdioma, (TipoBusqueda)pFilaCola.TipoBusqueda, -1, false, pParametros_Adiccionales);
                pCargadorResultados.RefrescarResultados(pFilaCola.ProyectoID, pFilaCola.ProyectoID.Equals(ProyectoAD.MetaProyecto), true, false, true, pIdioma, (TipoBusqueda)pFilaCola.TipoBusqueda, -1, false, pParametros_Adiccionales, true);
            }
            catch (Exception ex)
            {
                try
                {
                    pFilaCola.Estado = 1;
                    loggingService.GuardarLog("Error al refrescar los resultados de la fila " + pFilaCola.ColaID + " para el usuario conectado en idioma " + pIdioma + " ERROR:  Excepción: " + ex.ToString() + "\n\n\tTraza: " + ex.StackTrace);
                }
                catch (Exception) { }
            }

            try
            {
                // faceta etiquetas
                pCargadorFacetas.RefrescarFacetas(pFilaCola.ProyectoID, pFilaCola.ProyectoID.Equals(ProyectoAD.MetaProyecto), true, pUbicacionBusqueda, true, pIdioma, (TipoBusqueda)pFilaCola.TipoBusqueda, 1, pParametros_Adiccionales, false, null);
                pCargadorFacetas.RefrescarFacetas(pFilaCola.ProyectoID, pFilaCola.ProyectoID.Equals(ProyectoAD.MetaProyecto), true, pUbicacionBusqueda, true, pIdioma, (TipoBusqueda)pFilaCola.TipoBusqueda, 1, pParametros_Adiccionales, false, null, true);
            }
            catch (Exception ex)
            {
                try
                {
                    pFilaCola.Estado = 1;
                    loggingService.GuardarLog("Error al refrescar las facetas etiquetas de la fila " + pFilaCola.ColaID + " para el usuario conectado en idioma " + pIdioma + " ERROR:  Excepción: " + ex.ToString() + "\n\n\tTraza: " + ex.StackTrace);
                }
                catch (Exception) { }
            }

            try
            {
                // faceta categorias
                pCargadorFacetas.RefrescarFacetas(pFilaCola.ProyectoID, pFilaCola.ProyectoID.Equals(ProyectoAD.MetaProyecto), true, pUbicacionBusqueda, true, pIdioma, (TipoBusqueda)pFilaCola.TipoBusqueda, 2, pParametros_Adiccionales, false, null);
                pCargadorFacetas.RefrescarFacetas(pFilaCola.ProyectoID, pFilaCola.ProyectoID.Equals(ProyectoAD.MetaProyecto), true, pUbicacionBusqueda, true, pIdioma, (TipoBusqueda)pFilaCola.TipoBusqueda, 2, pParametros_Adiccionales, false, null, true);
            }
            catch (Exception ex)
            {
                try
                {
                    pFilaCola.Estado = 1;
                    loggingService.GuardarLog("Error al refrescar las facetas categorias de la fila " + pFilaCola.ColaID + " para el usuario conectado en idioma " + pIdioma + " ERROR:  Excepción: " + ex.ToString() + "\n\n\tTraza: " + ex.StackTrace);
                }
                catch (Exception) { }
            }

            try
            {
                // resto facetas
                pCargadorFacetas.RefrescarFacetas(pFilaCola.ProyectoID, pFilaCola.ProyectoID.Equals(ProyectoAD.MetaProyecto), true, pUbicacionBusqueda, true, pIdioma, (TipoBusqueda)pFilaCola.TipoBusqueda, 3, pParametros_Adiccionales, false, null);
                pCargadorFacetas.RefrescarFacetas(pFilaCola.ProyectoID, pFilaCola.ProyectoID.Equals(ProyectoAD.MetaProyecto), true, pUbicacionBusqueda, true, pIdioma, (TipoBusqueda)pFilaCola.TipoBusqueda, 3, pParametros_Adiccionales, false, null, true);
            }
            catch (Exception ex)
            {
                try
                {
                    pFilaCola.Estado = 1;
                    loggingService.GuardarLog("Error al refrescar el resto de facetas de la fila " + pFilaCola.ColaID + " para el usuario conectado en idioma " + pIdioma + " ERROR:  Excepción: " + ex.ToString() + "\n\n\tTraza: " + ex.StackTrace);
                }
                catch (Exception) { }
            }

            if (!pFilaCola.TipoBusqueda.Equals((short)TipoBusqueda.PersonasYOrganizaciones))
            {
                try
                {
                    // facetas home
                    pCargadorFacetas.RefrescarFacetas(pFilaCola.ProyectoID, pFilaCola.ProyectoID.Equals(ProyectoAD.MetaProyecto), true, "homeCatalogoParticular", true, pIdioma, (TipoBusqueda)pFilaCola.TipoBusqueda, 8, pParametros_Adiccionales, false, null);
                }
                catch (Exception ex)
                {
                    try
                    {
                        pFilaCola.Estado = 1;
                        loggingService.GuardarLog("Error al refrescar las facetas de la home de la fila " + pFilaCola.ColaID + " para el usuario conectado en idioma " + pIdioma + " ERROR:  Excepción: " + ex.ToString() + "\n\n\tTraza: " + ex.StackTrace);
                    }
                    catch (Exception) { }
                }
            }

            #endregion

            #region Invitado

            /*
                 * La caché de resultados del usuario conectado y del usuari invitado se comparten (tienen la misma clave de caché)
                 */
            //Usuario Invitado
            try
            {
                // resultados
                pCargadorResultados.RefrescarResultados(pFilaCola.ProyectoID, pFilaCola.ProyectoID.Equals(ProyectoAD.MetaProyecto), false, true, true, pIdioma, (TipoBusqueda)pFilaCola.TipoBusqueda, -1, false, pParametros_Adiccionales);
                pCargadorResultados.RefrescarResultados(pFilaCola.ProyectoID, pFilaCola.ProyectoID.Equals(ProyectoAD.MetaProyecto), false, true, true, pIdioma, (TipoBusqueda)pFilaCola.TipoBusqueda, -1, false, pParametros_Adiccionales, true);
            }
            catch (Exception ex)
            {
                try
                {
                    pFilaCola.Estado = 1;
                    loggingService.GuardarLog("Error al refrescar los resultados de la fila " + pFilaCola.ColaID + " para el usuario Invitado en idioma " + pIdioma + " ERROR:  Excepción: " + ex.ToString() + "\n\n\tTraza: " + ex.StackTrace);
                }
                catch (Exception) { }
            }

            try
            {
                // faceta etiquetas
                pCargadorFacetas.RefrescarFacetas(pFilaCola.ProyectoID, pFilaCola.ProyectoID.Equals(ProyectoAD.MetaProyecto), false, pUbicacionBusqueda, true, pIdioma, (TipoBusqueda)pFilaCola.TipoBusqueda, 1, pParametros_Adiccionales, false, null);
                pCargadorFacetas.RefrescarFacetas(pFilaCola.ProyectoID, pFilaCola.ProyectoID.Equals(ProyectoAD.MetaProyecto), false, pUbicacionBusqueda, true, pIdioma, (TipoBusqueda)pFilaCola.TipoBusqueda, 1, pParametros_Adiccionales, false, null, true);
            }
            catch (Exception ex)
            {
                try
                {
                    pFilaCola.Estado = 1;
                    loggingService.GuardarLog("Error al refrescar las facetas etiquetas de la fila " + pFilaCola.ColaID + " para el usuario Invitado en idioma " + pIdioma + " ERROR:  Excepción: " + ex.ToString() + "\n\n\tTraza: " + ex.StackTrace);
                }
                catch (Exception) { }
            }

            try
            {
                // faceta categorias
                pCargadorFacetas.RefrescarFacetas(pFilaCola.ProyectoID, pFilaCola.ProyectoID.Equals(ProyectoAD.MetaProyecto), false, pUbicacionBusqueda, true, pIdioma, (TipoBusqueda)pFilaCola.TipoBusqueda, 2, pParametros_Adiccionales, false, null);
                pCargadorFacetas.RefrescarFacetas(pFilaCola.ProyectoID, pFilaCola.ProyectoID.Equals(ProyectoAD.MetaProyecto), false, pUbicacionBusqueda, true, pIdioma, (TipoBusqueda)pFilaCola.TipoBusqueda, 2, pParametros_Adiccionales, false, null, true);
            }
            catch (Exception ex)
            {
                try
                {
                    pFilaCola.Estado = 1;
                    loggingService.GuardarLog("Error al refrescar las facetas categorias de la fila " + pFilaCola.ColaID + " para el usuario Invitado en idioma " + pIdioma + " ERROR:  Excepción: " + ex.ToString() + "\n\n\tTraza: " + ex.StackTrace);
                }
                catch (Exception) { }
            }

            try
            {
                // resto facetas
                pCargadorFacetas.RefrescarFacetas(pFilaCola.ProyectoID, pFilaCola.ProyectoID.Equals(ProyectoAD.MetaProyecto), false, pUbicacionBusqueda, true, pIdioma, (TipoBusqueda)pFilaCola.TipoBusqueda, 3, pParametros_Adiccionales, false, null);
                pCargadorFacetas.RefrescarFacetas(pFilaCola.ProyectoID, pFilaCola.ProyectoID.Equals(ProyectoAD.MetaProyecto), false, pUbicacionBusqueda, true, pIdioma, (TipoBusqueda)pFilaCola.TipoBusqueda, 3, pParametros_Adiccionales, false, null, true);
            }
            catch (Exception ex)
            {
                try
                {
                    pFilaCola.Estado = 1;
                    loggingService.GuardarLog("Error al refrescar el resto de facetas de la fila " + pFilaCola.ColaID + " para el usuario Invitado en idioma " + pIdioma + " ERROR:  Excepción: " + ex.ToString() + "\n\n\tTraza: " + ex.StackTrace);
                }
                catch (Exception) { }
            }

            if (!pFilaCola.TipoBusqueda.Equals((short)TipoBusqueda.PersonasYOrganizaciones))
            {
                try
                {
                    // facetas home
                    pCargadorFacetas.RefrescarFacetas(pFilaCola.ProyectoID, pFilaCola.ProyectoID.Equals(ProyectoAD.MetaProyecto), false, "homeCatalogoParticular", true, pIdioma, (TipoBusqueda)pFilaCola.TipoBusqueda, 8, pParametros_Adiccionales, false, null);
                }
                catch (Exception ex)
                {
                    try
                    {
                        pFilaCola.Estado = 1;
                        loggingService.GuardarLog("Error al refrescar las facetas de la home de la fila " + pFilaCola.ColaID + " para el usuario Invitado en idioma " + pIdioma + " ERROR:  Excepción: " + ex.ToString() + "\n\n\tTraza: " + ex.StackTrace);
                    }
                    catch (Exception) { }
                }
            }

            #endregion

            #region Bot

            //Bot
            try
            {
                // Resultados
                pCargadorResultados.RefrescarResultados(pFilaCola.ProyectoID, pFilaCola.ProyectoID.Equals(ProyectoAD.MetaProyecto), false, true, true, pIdioma, (TipoBusqueda)pFilaCola.TipoBusqueda, -1, true, pParametros_Adiccionales);
            }
            catch (Exception ex)
            {
                try
                {
                    pFilaCola.Estado = 1;
                    loggingService.GuardarLog("Error al refrescar los resultados de la fila " + pFilaCola.ColaID + " para el bot en idioma " + pIdioma + " ERROR:  Excepción: " + ex.ToString() + "\n\n\tTraza: " + ex.StackTrace);
                }
                catch (Exception) { }
            }

            try
            {
                // Facetas
                pCargadorFacetas.RefrescarFacetas(pFilaCola.ProyectoID, pFilaCola.ProyectoID.Equals(ProyectoAD.MetaProyecto), false, pUbicacionBusqueda, true, pIdioma, (TipoBusqueda)pFilaCola.TipoBusqueda, 1, pParametros_Adiccionales, true, null);
            }
            catch (Exception ex)
            {
                try
                {
                    pFilaCola.Estado = 1;
                    loggingService.GuardarLog("Error al refrescar las facetas de la fila " + pFilaCola.ColaID + " para el bot en idioma " + pIdioma + " ERROR:  Excepción: " + ex.ToString() + "\n\n\tTraza: " + ex.StackTrace);
                }
                catch (Exception) { }
            }

            #endregion
        }

        //private ParametroGeneralDS.ParametroGeneralRow ObtenerFilaParametroGeneral(Guid pProyectoID)
        //{
        //    ParametroGeneralDS.ParametroGeneralRow filaParametroGeneral = null;

        //    ParametroGeneralCL paramCL = new ParametroGeneralCL(mFicheroConfiguracionBD);
        //    ParametroGeneralDS paramDS = paramCL.ObtenerParametrosGeneralesDeProyecto(pProyectoID);
        //    paramCL.Dispose();

        //    if (paramDS != null && paramDS.ParametroGeneral.Select("ProyectoID = '" + pProyectoID + "'")[0] != null)
        //    {
        //        filaParametroGeneral = (ParametroGeneralDS.ParametroGeneralRow)paramDS.ParametroGeneral.Select("ProyectoID = '" + pProyectoID + "'")[0];
        //    }

        //    return filaParametroGeneral;
        //}
        private ParametroGeneral ObtenerFilaParametroGeneral(Guid pProyectoID, EntityContext entityContext, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            ParametroGeneral filaParametroGeneral = null;

            ParametroGeneralCL paramCL = new ParametroGeneralCL(entityContext, loggingService, redisCacheWrapper, mConfigService, servicesUtilVirtuosoAndReplication);
            GestorParametroGeneral gestorParametroGeneral = new GestorParametroGeneral();
            //ParametroAplicacionGBD parametroAplicacionGBD = new ParametroAplicacionGBD();
            //gestorParametroGeneral= parametroAplicacionGBD.ObtenerParametrosGeneralesDeProyecto(gestorParametroGeneral,pProyectoID);
            gestorParametroGeneral = paramCL.ObtenerParametrosGeneralesDeProyecto(pProyectoID);
            //paramCL.Dispose();

            ParametroGeneral parametroGeneralBusqueda = gestorParametroGeneral.ListaParametroGeneral.Where(parametroGeneral => parametroGeneral.ProyectoID.Equals(pProyectoID)).FirstOrDefault();

            if (gestorParametroGeneral != null && parametroGeneralBusqueda != null)
            {
                filaParametroGeneral = parametroGeneralBusqueda;
            }

            return filaParametroGeneral;
        }

        #region Refresco de caché de componentes


        private void RefrescarCacheComponente(Guid pComponenteID, Guid pProyectoID, EntityContext entityContext, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {

            CMSComponente filaComponente = null;
            CMSCN cmsCN = new CMSCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            DataWrapperCMS cmsDW = cmsCN.ObtenerComponentePorID(pComponenteID, pProyectoID);
            if (cmsDW.ListaCMSComponente.Count > 0)
            {
                filaComponente = cmsCN.ObtenerComponentePorID(pComponenteID, pProyectoID).ListaCMSComponente.FirstOrDefault();
            }
            cmsCN.Dispose();

            RefrescarCacheComponente(filaComponente, entityContext, loggingService, redisCacheWrapper, servicesUtilVirtuosoAndReplication);
        }

        private void RefrescarCacheComponente(CMSComponente filaComponente, EntityContext entityContext, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            RealizarPeticionWebRefrescoCache(filaComponente, entityContext, loggingService, redisCacheWrapper, servicesUtilVirtuosoAndReplication);
        }

        private void ActualizarFechaProximaActualizacionComponente(CMSComponente pFilaComponente, DateTime pFechaActualizacion, bool pActualizarBD, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            if (pActualizarBD)
            {
                CMSCN cmsCN = new CMSCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                cmsCN.ActualizarCaducidadComponente(pFilaComponente.ComponenteID, pFechaActualizacion);
                cmsCN.Dispose();
            }
        }

        private string ObtenerDominioComunidad(Guid pOrganizacionID, Guid pProyectoID, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            if (!mListaUrlPorProyecto.ContainsKey(pProyectoID))
            {
                ProyectoCN proyCN = new ProyectoCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                string dominio = proyCN.ObtenerURLPropiaProyecto(pProyectoID);
                proyCN.Dispose();
                //string nombreCorto = proyCN.ObtenerNombreCortoProyecto(pOrganizacionID, pProyectoID);

                //string url = dominio + "/comunidad/" + nombreCorto;
                mListaUrlPorProyecto.Add(pProyectoID, dominio);
            }

            return mListaUrlPorProyecto[pProyectoID];
        }

        private string ObtenerURLRefrescoComponente(CMSComponente pFilaComponente, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            return ObtenerDominioComunidad(pFilaComponente.OrganizacionID, pFilaComponente.ProyectoID, entityContext, loggingService, servicesUtilVirtuosoAndReplication).TrimEnd('/') + "/cmspagina?proyectoid=" + pFilaComponente.ProyectoID + "&componenteid=" + pFilaComponente.ComponenteID;
        }

        private void RealizarPeticionWebRefrescoCache(CMSComponente pFilaComponente, EntityContext entityContext, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            if (pFilaComponente != null)
            {
                try
                {
                    //Obtengo la url de refresco y hago una petición web
                    string urlRefresco = ObtenerURLRefrescoComponente(pFilaComponente, entityContext, loggingService, servicesUtilVirtuosoAndReplication);

                    string urlHost = new Uri(urlRefresco).Host;

                    if (!mPeticionesWebActuales.ContainsKey(urlHost))
                    {
                        //Añado el dominio a la lista de peticiones
                        mPeticionesWebActuales.Add(urlHost, new List<string>());
                    }
                    else if (mPeticionesWebActuales[urlHost].Contains(urlRefresco))
                    {
                        //Esta petición ya está en marcha, no la voy a hacer 2 veces porque el efecto sería el mismo
                        return;
                    }

                    int segundosEsperando = 0;
                    while (mPeticionesWebActuales[urlHost].Count >= mNumeroMaxPeticionesWebSimultaneas)
                    {
                        if (segundosEsperando < 10)
                        {
                            //Hemos llegado al máximo de peticiones web concurrentes permitidas a este dominio, toca esperar hasta que termine alguna
                            Thread.Sleep(1000);
                            segundosEsperando++;
                        }
                        else
                        {
                            throw new Exception();
                        }
                    }

                    mPeticionesWebActuales[urlHost].Add(urlRefresco);
                    RealizarPeticionWebRefrescoCache realizarPeticion = new RealizarPeticionWebRefrescoCache(ScopedFactory, mConfigService);
                    realizarPeticion.FilaComponente = pFilaComponente;

                    realizarPeticion.UrlRefrescoComponente = urlRefresco;
                    realizarPeticion.PeticionesWebActuales = mPeticionesWebActuales;

                    Task.Factory.StartNew(realizarPeticion.EmpezarMantenimiento);

                    Thread.Sleep(1000);
                }
                catch
                {
                    CMSCL cmsCL = new CMSCL(entityContext, loggingService, redisCacheWrapper, mConfigService, servicesUtilVirtuosoAndReplication);
                    cmsCL.InvalidarCacheDeComponentePorIDEnProyectoTodosIdiomas(pFilaComponente.ProyectoID, pFilaComponente.ComponenteID);
                    cmsCL.Dispose();
                }
            }
        }

        #endregion

        private List<CMSComponente> CargarComponentesProyectoCaducidadRecurso(Guid pProyectoID, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            CMSCN cmsCN = new CMSCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            return cmsCN.ObtenerFilasComponentesCMSDeProyectoPorTipoCaducidad(pProyectoID, TipoCaducidadComponenteCMS.Recurso);
        }


        private List<CMSComponente> CargarComponentesCaducados(EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            CMSCN cmsCN = new CMSCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            return cmsCN.ObtenerFilasComponentesCMSCaducados();
        }

        //private void CargarComponentesProyecto(Guid pProyectoID)
        //{
        //    CMSCN cmsCN = new CMSCN(mFicheroConfiguracionBD);
        //    mCmsDS = cmsCN.ObtenerComponentesCMSDeProyecto(pProyectoID);


        //    foreach (CMSDS.CMSComponenteRow filaComponente in mCmsDS.CMSComponente)
        //    {
        //        TipoCaducidadComponenteCMS tipoCaducidad = (TipoCaducidadComponenteCMS)filaComponente.TipoCaducidadComponente;
        //        if (ComprobarTipoCaducidadComponenteCaducaPorTiempo(tipoCaducidad))
        //        {
        //            //Reviso la fecha de actualización, si ya se ha pasado, la fecha de actualización será ahora mismo
        //            DateTime fechaActualizacion = DateTime.Now;
        //            if (!filaComponente.IsFechaUltimaActualizacionNull() && filaComponente.FechaUltimaActualizacion > DateTime.Now)
        //            {
        //                //La fecha de actualización es posterior a este momento, lo pongo para que se actualice cuando le toque
        //                fechaActualizacion = filaComponente.FechaUltimaActualizacion;
        //            }
        //            ListaFechasCaducidadComponentes.Add(filaComponente.ComponenteID, fechaActualizacion);
        //        }
        //        else if (tipoCaducidad.Equals(TipoCaducidadComponenteCMS.Recurso))
        //        {
        //            if (!ListaComponentesCaducidadRecursoPorProyecto.ContainsKey(filaComponente.ProyectoID))
        //            {
        //                ListaComponentesCaducidadRecursoPorProyecto.Add(filaComponente.ProyectoID, new List<CMSDS.CMSComponenteRow>());
        //            }
        //            ListaComponentesCaducidadRecursoPorProyecto[filaComponente.ProyectoID].Add(filaComponente);
        //        }
        //        ListaComponentes.Add(filaComponente.ComponenteID, filaComponente);
        //    }
        //}

        /// <summary>
        /// Comprueba si un tipo de caducidad es temporal, es decir, es diario, por horas o por semanas
        /// </summary>
        /// <param name="pTipoCaducidad">Tipo de caducidad a comprobar</param>
        /// <returns></returns>
        private bool ComprobarTipoCaducidadComponenteCaducaPorTiempo(TipoCaducidadComponenteCMS pTipoCaducidad)
        {
            return pTipoCaducidad.Equals(TipoCaducidadComponenteCMS.Dia) || pTipoCaducidad.Equals(TipoCaducidadComponenteCMS.Hora) || pTipoCaducidad.Equals(TipoCaducidadComponenteCMS.Semana);
        }

        #endregion

        #region Propiedades

        ///// <summary>
        ///// Obtiene las fechas en las que caduca cada componente
        ///// </summary>
        //private Dictionary<Guid, List<CMSDS.CMSComponenteRow>> ListaComponentesCaducidadRecursoPorProyecto
        //{
        //    get
        //    {
        //        if (mListaComponentesCaducidadRecursoPorProyecto == null)
        //        {
        //            CargarComponentes();
        //        }
        //        return mListaComponentesCaducidadRecursoPorProyecto;
        //    }
        //}

        ///// <summary>
        ///// Obtiene las fechas en las que caduca cada componente
        ///// </summary>
        //private Dictionary<Guid, DateTime> ListaFechasCaducidadComponentes
        //{
        //    get
        //    {
        //        if (mListaFechasCaducidadComponentes == null)
        //        {
        //            CargarComponentes();
        //        }
        //        return mListaFechasCaducidadComponentes;
        //    }
        //}

        ///// <summary>
        ///// Obtiene la lista de todos los componentes
        ///// </summary>
        //private Dictionary<Guid, CMSDS.CMSComponenteRow> ListaComponentes
        //{
        //    get
        //    {
        //        if (mListaComponentes == null)
        //        {

        //        }
        //        return mListaComponentes;
        //    }
        //}

        #endregion
    }
}
