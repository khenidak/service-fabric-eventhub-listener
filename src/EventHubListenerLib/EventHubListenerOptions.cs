using EventHubListenerLib.Common;
using System;
using System.Collections.Generic;
using System.Fabric;
using System.Fabric.Query;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventHubListenerLib
{
    /// <summary>
    /// describes how an Event Hub Listener will operate. 
    /// </summary>
    public sealed class EventHubListenerOptions
    {
        private Uri mServiceName;
        private FabricClient mFabricClient;
        private string mCurrentSFPartitionId;


        internal long Epoch = long.MinValue;
        internal bool IsEpoch = false;

        internal string Offset;
        internal bool IsOffsetInclusive = false;
        internal bool IsOffset;


        internal DateTime StartUTCDateTime;
        internal bool IsStartUTCDateTime = false;
           

        public string CurrentSFPartitionId { get { return mCurrentSFPartitionId; } }
        public string EventHubName { get; set; }
        public string EventHubConnectionString { get; set; }
        public string EventHubConsumerGroupName { get; set; }
        public EventHubListenerMode ListenerMode { get; set; } = EventHubListenerMode.SafeDistribute;
        public IEventHubPartitionStateFactory StateFactory { get; set; }
        public string[] OrderedServicePartitionIds { get; set; }
        public string AssignedEventHubPartitionId { get; set; }

        public IEventHubEventsProcessor Processor { get; set; }

        public int BatchSize { get; set; } = 200;

        
        private async Task SetServicePartitionListAsync()
        {
            ServicePartitionList PartitionList = await mFabricClient.QueryManager.GetPartitionListAsync(mServiceName);

            List<string> partitions = new List<string>();

            foreach (Partition p in PartitionList)
                partitions.Add(p.PartitionInformation.Id.ToString());

            OrderedServicePartitionIds = partitions.OrderBy(s => s).ToArray();
        }

        private string[] DistributeOverServicePartitions(string[] orderEventHubPartition)
        {
            // service partitions are greater or equal
            // in this case each service partition gets an event hub partitions
            // the reminder partitions will just not gonna work on anything. 
            if (OrderedServicePartitionIds.Length >= orderEventHubPartition.Length)
            {
                int servicePartitionRank = Array.IndexOf(OrderedServicePartitionIds, CurrentSFPartitionId);

                if(servicePartitionRank < orderEventHubPartition.Length)
                    return new string[] { orderEventHubPartition[servicePartitionRank] };


                return new string[0];
            }
            else
            {
                // service partitions are less than event hub partitins, distribute.. 
                // service partitions can be odd or even. 

                int reminder = orderEventHubPartition.Length % OrderedServicePartitionIds.Length;
                int HubPartitionsPerServicePartitions = orderEventHubPartition.Length / OrderedServicePartitionIds.Length;
                int servicePartitionRank = Array.IndexOf(OrderedServicePartitionIds, CurrentSFPartitionId);

                List<string> assignedIds = new List<string>();
                for (int i = 0; i < HubPartitionsPerServicePartitions; i++)
                {
                    assignedIds.Add(orderEventHubPartition[(servicePartitionRank * HubPartitionsPerServicePartitions) + i]);
                }

                // last service partition gets the reminder
                if (servicePartitionRank == (OrderedServicePartitionIds.Length - 1))
                {
                    for (int i = reminder; i > 0; i--)
                    {
                        assignedIds.Add(orderEventHubPartition[orderEventHubPartition.Length - i]);
                    }
                }

                return assignedIds.ToArray();
            }
        }

        internal async Task PrepareAsync()
        {
            if (IsStartUTCDateTime && IsOffset)
                throw new InvalidOperationException("can not have start utc date && start offset at the same time, either one of them or none of them");


            if (EventHubListenerMode.Single == ListenerMode && string.IsNullOrEmpty(AssignedEventHubPartitionId))
                throw new InvalidOperationException("Listener is in Single mode and no assigned event hub partition id");

            if (null != OrderedServicePartitionIds)
            {

                if (OrderedServicePartitionIds.Where(p => null == p).Count() > 0)
                    throw new InvalidOperationException("Partiton list contains one or more null entries");

            }
            else
            {
                if (null == mServiceName)
                    throw new InvalidOperationException("Can not resolve partitions list with no service fabric service name assigned");

                await SetServicePartitionListAsync();
            }


            if (null == Processor)
                throw new InvalidOperationException("processor is null");


            if (null == StateFactory)
                throw new InvalidOperationException("state factor is null");


            if (string.IsNullOrEmpty(EventHubName))
                throw new InvalidOperationException("Event hub name is null or empty");

            if (string.IsNullOrEmpty(EventHubConnectionString))
                throw new InvalidOperationException("Event hub connection string is null or empty");

            if (null == EventHubConsumerGroupName)
                EventHubConsumerGroupName = string.Empty;
        }
        
        internal string[] ResolveAssignedEventHubPartitions(string[] EventHubPartitionsIds)
        {
            string[] orderedEventHubPartitionIds = EventHubPartitionsIds.OrderBy((s) => s).ToArray();


            switch (ListenerMode)
            {
                case EventHubListenerMode.Single:
                    {
                        if (!orderedEventHubPartitionIds.Contains(this.AssignedEventHubPartitionId))
                            throw new InvalidOperationException(string.Format("Assgined Event hub Partition {0} is not found", AssignedEventHubPartitionId));


                        return new string[] { this.AssignedEventHubPartitionId };
                    }
                case EventHubListenerMode.OneToOne:
                    {

                        if (OrderedServicePartitionIds.Length != orderedEventHubPartitionIds.Length)
                            throw new InvalidOperationException("Event Hub listener is in 1:1 mode yet servie partitions are not equal to event hub partitions");
                        int servicePartitionRank = Array.IndexOf(OrderedServicePartitionIds, mCurrentSFPartitionId);

                        return new string[] { orderedEventHubPartitionIds[servicePartitionRank] };
                    }
                case EventHubListenerMode.Distribute:
                    {
                        return DistributeOverServicePartitions(orderedEventHubPartitionIds);
                    }
                case EventHubListenerMode.SafeDistribute:
                    {
                        // we can work with service partitions < or = Event Hub partitions 
                        // anything else is an error case

                        if (OrderedServicePartitionIds.Length > orderedEventHubPartitionIds.Length)
                            throw new InvalidOperationException("Event Hub listener is in fairDistribute mode yet servie partitions greater than event hub partitions");


                        return DistributeOverServicePartitions(orderedEventHubPartitionIds);
                    }
                default:
                    {
                        throw new InvalidOperationException(string.Format("can not resolve event hub partition for {0}", this.ListenerMode.ToString()));
                    }
            }


            throw new InvalidOperationException("could not resolve event hub partitions");
        }
    
        public EventHubListenerOptions UseEpoch(long epoch)
        {
            Epoch = epoch;
            IsEpoch = true;
            return this;
        }

        public EventHubListenerOptions StartWithOffset(string offSet, bool IsInclusive)
        {
            Offset = offSet;
            IsOffsetInclusive = IsInclusive;
            IsOffset = true;
            return this;
        }

        public EventHubListenerOptions StartWithEventsNewerThan(DateTime startUTCDateTime)
        {
            StartUTCDateTime = startUTCDateTime;
            IsStartUTCDateTime = true;
            return this;
        }

        public EventHubListenerOptions(string currentPartitionId, Uri serviceName, FabricClient fabricClient = null) : this(currentPartitionId) 
        {
            mServiceName  = serviceName;
            mFabricClient = null == fabricClient ? new FabricClient() : fabricClient;
        }


        public EventHubListenerOptions(string currentPartitionId)
        {
            if (string.IsNullOrEmpty(currentPartitionId))
                throw new ArgumentNullException(nameof(currentPartitionId));

            mCurrentSFPartitionId = currentPartitionId;
        }


      




    }
}
