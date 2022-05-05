using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Es.Riam.Gnoss.Win.RefrescoCache
{
    public class BusquedaRefrescoCaducidad
    {
        public Guid ProyectoID { get; set; }
        public short TipoBusqueda { get; set; }
        public DateTime Caducidad { get; set; }
    }
}
