![](https://content.gnoss.ws/imagenes/proyectos/personalizacion/7e72bf14-28b9-4beb-82f8-e32a3b49d9d3/cms/logognossazulprincipal.png)

# Gnoss.BackgroundTask.CacheRefresh.OpenCORE

Aplicación de segundo plano que se encarga de invalidar las cachés que necesitan ser actualizadas. Por ejemplo, actualiza las primeras páginas de búsqueda, los componentes de recursos de la home de una comunidad cuando se ha publicado un recurso nuevo, etc., para que aparezcan los nuevos recursos.  

Configuración estandar de esta aplicación en el archivo docker-compose.yml: 

```yml
cacherefresh:
    image: cacherefresh
    env_file: .env
    environment:
     virtuosoConnectionString: ${virtuosoConnectionString}
     acid: ${acid}
     base: ${base}
     RabbitMQ__colaServiciosWin: ${RabbitMQ}
     RabbitMQ__colaReplicacion: ${RabbitMQ}
     redis__redis__ip__master: ${redis__redis__ip__master}
     redis__redis__bd: ${redis__redis__bd}
     redis__redis__timeout: ${redis__redis__timeout}
     redis__recursos__ip__master: ${redis__recursos__ip__master}
     redis__recursos__bd: ${redis__recursos_bd}
     redis__recursos__timeout: ${redis__recursos_timeout}
     redis__liveUsuarios__ip__master: ${redis__liveUsuarios__ip__master}
     redis__liveUsuarios__bd: ${redis__liveUsuarios_bd}
     redis__liveUsuarios__timeout: ${redis__liveUsuarios_timeout}
     idiomas: "es|Español,en|English"
     tipoBD: 0
     Servicios__urlBase: ${url_servicioBase}
     Servicios__urlFacetas: ${url_servicioFacetas}
     Servicios__urlResultados: ${url_servicioResutlados}
     connectionType: "0"
     intervalo: "100"
    volumes:
     - ./logs/cacherefresh:/app/logs
```

Se pueden consultar los posibles valores de configuración de cada parámetro aquí: https://github.com/equipognoss/Gnoss.Platform.Deploy
