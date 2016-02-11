using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventHubListenerLib.Common
{
    public interface IEventHubPartitionStateFactory
    {
        Task<IEventHubPartitionState> GetOrCreateAsync(string PartitionId);
    }

}
