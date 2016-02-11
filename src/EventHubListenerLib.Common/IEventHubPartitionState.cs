using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventHubListenerLib.Common
{
    public interface IEventHubPartitionState
    {
        string PartitionId { get; set; }
        string Offset {get; set;}

         Task SaveAsync();

        
    }
}
