using EventHubListenerLib.Common;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace EventHubListenerLib
{
    /// <summary>
    /// Default implementation of IEventHubPartitionState
    /// </summary>
    public sealed class DefaultPartitionState : IEventHubPartitionState
    {
        [IgnoreDataMember]
        internal IReliableStateManager StateManager { get; set; }
        [IgnoreDataMember]
        public string EntryName { get; internal set; }

        [IgnoreDataMember]
        public IReliableDictionary<string, DefaultPartitionState> StateStore { get; internal set; }
        
        public string Offset { get; set; }
        public string PartitionId{get; set;}

        public async Task SaveAsync()
        {
            using (var tx = StateManager.CreateTransaction())
            {
                var result = await StateStore.AddOrUpdateAsync(tx, EntryName, this, (k,v) =>
                {
                    return this;
                }
                );
                
                await tx.CommitAsync();
            }
        }
    }
}
