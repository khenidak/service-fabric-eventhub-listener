using Microsoft.ServiceFabric.Services.Communication.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using EventHubListenerLib.Common;
using System.Fabric;
using System.Fabric.Query;
using Microsoft.ServiceBus.Messaging;
using System.Diagnostics;

namespace EventHubListenerLib
{
    public sealed class EventHubListener : ICommunicationListener
    {


        private EventHubListenerOptions mOptions;
        private MessagingFactory mMessagingFactory;
        private EventHubClient mEventHubClient;
        private EventHubConsumerGroup mConsumerGroup;
        private List<EventHubListenerPartitionReceiver> mReceivers = new List<EventHubListenerPartitionReceiver>();
        private string mEventHubNamespace;

        private string EventHubNamespace
        {
            get
            {
                if (string.IsNullOrEmpty(mEventHubNamespace))
                {
                    string[] elements = mOptions.EventHubConnectionString.Split(';');

                    foreach (string elem in elements)
                    {
                        if (elem.ToLowerInvariant().StartsWith("endpoint="))
                        {
                            mEventHubNamespace = new Uri(elem.Split('=')[1]).Host;
                        }
                    }
                }
                return mEventHubNamespace;
            }
        }



        public EventHubListener(EventHubListenerOptions options)
        {
            if (null == options)
                throw new ArgumentNullException(nameof(options));

            mOptions = options;
        }

        
        public void Abort()
        {
            try
            {
                if (null != mMessagingFactory && !mMessagingFactory.IsClosed)
                    mMessagingFactory.Close();

            }
            catch
            {
                //no op
            }
            finally
            {
                mReceivers.ForEach(r => r = null);
                mReceivers.Clear();

            }


        }

        public async Task CloseAsync(CancellationToken cancellationToken)
        {
            var tasks = new List<Task>();

            foreach (var r in mReceivers)
                tasks.Add(r.StopAsync());

            if (null != mMessagingFactory && !mMessagingFactory.IsClosed)
                tasks.Add(mMessagingFactory.CloseAsync());

            await Task.WhenAll(tasks);

            mReceivers.ForEach(r => r = null);
            mReceivers.Clear();


        }

       


        public async Task<string> OpenAsync(CancellationToken cancellationToken)
        {
            await mOptions.PrepareAsync();

            var useDefaultConsumerGroup = !string.IsNullOrEmpty(mOptions.EventHubConsumerGroupName);


            mMessagingFactory = MessagingFactory.CreateFromConnectionString(mOptions.EventHubConnectionString);
            mEventHubClient = mMessagingFactory.CreateEventHubClient(mOptions.EventHubName);
            mConsumerGroup = useDefaultConsumerGroup ?
                                mEventHubClient.GetConsumerGroup(mOptions.EventHubConsumerGroupName)
                              : mEventHubClient.GetDefaultConsumerGroup();

           


            return string.Concat(mEventHubNamespace, "/",
                                 mOptions.EventHubName, "/",
                                 useDefaultConsumerGroup ? "<default group>" : mOptions.EventHubConsumerGroupName);

        }

        /// <summary>
        ///  When called Event Hub receivers will be created and connect 
        ///  to Event Hub to receive events.
        /// </summary>
        /// <returns></returns>
        public async Task StartAsync()
        {
            // slice the pie according to distribution
            // this partition can get one or more assigned Event Hub Partition ids
            string[] EventHubPartitionIds = mEventHubClient.GetRuntimeInformation().PartitionIds;
            string[] ResolvedEventHubPartitionIds = mOptions.ResolveAssignedEventHubPartitions(EventHubPartitionIds);

            foreach (var resolvedPartition in ResolvedEventHubPartitionIds)
            {
                var rcver = new EventHubListenerPartitionReceiver(resolvedPartition, mOptions, mConsumerGroup);
                await rcver.StartAsync();
                mReceivers.Add(rcver);
            }

        }
    }
}
