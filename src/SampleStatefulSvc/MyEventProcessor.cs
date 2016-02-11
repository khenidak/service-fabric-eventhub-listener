using EventHubListenerLib.Common;
using Microsoft.ServiceBus.Messaging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SampleStatefulSvc
{
    /// <summary>
    /// sample implementation of IEventHubEventsProcessor all events (from all partitions) 
    /// are delivered to an object that implements IEventHubEventsProcessor 
    /// each batch of events is delivered from one partitions. 
    /// 
    /// ProcessEventsAsync will be called concurrently 
    /// </summary>
    class myEventProcessor : IEventHubEventsProcessor
    {
        public Task<bool> ProcessEventsAsync(IEnumerable<EventData> events, IEventHubPartitionState state)
        {


            foreach (var evt in events)
            {
                ServiceEventSource.Current.Message("Got Event:{0}", Encoding.UTF8.GetString(evt.GetBytes()));

                /*
                    optionally update the state on event by event bases. 
                    state.Offset = evt.Offset;
                    await state.SaveAsync();
                */
            }

            // or tell the listener to update the state batch by batch
            return Task.FromResult(true);

            // if you return false here, you are responsible to save the state when you see fit

        }
    }
}
