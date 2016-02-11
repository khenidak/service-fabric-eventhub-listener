using EventHubListenerLib.Common;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventHubListenerLib
{
    /// <summary>
    /// default implementation of IEventHubPartitionStateFactory that uses
    /// Service Fabric reliable state. It uses an IReliableDictionary<string, DefaultPartitionState>
    /// CTORs are provided to allow you to overide diciotnary name and/or entry name prefix.
    /// </summary>
    public sealed class DefaultPartitionStateFactory : IEventHubPartitionStateFactory
    {
        private IReliableStateManager mStateManager = null;

        public static readonly string DEFAULT_DICTIONARY_NAME = "EventHubPartitionState";
        public string EntriesPrefix { get; private set; } 
        public string ReliableDictionaryName { get; private set; } 
        public IReliableDictionary<string, DefaultPartitionState> StateStore{ get; private set; } = null;


        
        public DefaultPartitionStateFactory(IReliableStateManager stateManager) : this(stateManager,string. Empty)
        {

        }
        public DefaultPartitionStateFactory(IReliableStateManager stateManager, string reliableDictionaryName) : this (stateManager, reliableDictionaryName, string.Empty)
        {

        }

        public DefaultPartitionStateFactory(IReliableStateManager stateManager, string reliableDictionaryName, string entriesPrefix)
        {
            
            if(null == reliableDictionaryName)
                throw new ArgumentNullException(nameof(reliableDictionaryName));


            if (null == entriesPrefix)
                throw new ArgumentNullException(nameof(entriesPrefix));


            mStateManager = stateManager;
            ReliableDictionaryName = string.Empty == reliableDictionaryName ? DEFAULT_DICTIONARY_NAME : reliableDictionaryName;
            EntriesPrefix = entriesPrefix;
        }




        public  async Task<IEventHubPartitionState> GetOrCreateAsync(string PartitionId)
        {
            if (null == mStateManager)
                throw new InvalidOperationException("assigned state manager is null");



            if (null == StateStore)
                StateStore = await mStateManager.GetOrAddAsync<IReliableDictionary<string, DefaultPartitionState>>(ReliableDictionaryName);


            if (null == StateStore)
                throw new InvalidOperationException("could not create a reliable dictionary to store partition state");


            var entryname = string.Concat(EntriesPrefix, PartitionId);
            DefaultPartitionState partitionState = null;
            using (var tx = mStateManager.CreateTransaction())
            {
                var result = await StateStore.TryGetValueAsync(tx, entryname);
                if (result.HasValue)
                {
                    partitionState = result.Value;
                }
                else
                {
                    partitionState = new DefaultPartitionState();
                    partitionState.PartitionId = PartitionId;
                }
                await tx.CommitAsync();
            }
            partitionState.EntryName = entryname;
            partitionState.StateStore = StateStore;
            partitionState.StateManager = mStateManager;


            return partitionState;
        }
    }
}
