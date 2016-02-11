using Microsoft.ServiceBus.Messaging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventHubListenerLib.Common
{
    public interface IEventHubEventsProcessor
    {
        Task<bool> ProcessEventsAsync(IEnumerable<EventData> events, IEventHubPartitionState state);

    }
}
