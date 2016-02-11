using EventHubListenerLib;
using EventHubListenerLib.Common;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;
using System.Text;
using System.Diagnostics;

namespace SampleStatefulSvc
{
    
    internal sealed class SampleStatefulSvc : StatefulService
    {
        private EventHubListener mEventHubListener;
        private string mEventHubConnectionString = "Endpoint=sb://{Namespace}.servicebus.windows.net/;SharedAccessKeyName={KeyName};SharedAccessKey={Key};TransportType=Amqp";
        private string mEventHubName = "{Event Hub Name}";

        private ICommunicationListener CreateEventHubListener()
        {


            /*********************************************************************/
            // 1: Create a state factory
            /*********************************************************************/
            /*
              as you read events from event hub you need to maintain a state (combination of partition + offset in the partition). this state is 
              a marker to last event you have read (in order to avoid to duplicates). 

              default state factory uses Service Fabric Reliable State to store state as in IReliableDictoinary<string, DefaultPartitionState>
            
              You can implement different PartitionStateFactory & Partition State (example: using the listener in a stateless service). 
            */

            var factory = new DefaultPartitionStateFactory(this.StateManager); // other overloads allows you to override default dictionary name
                                                                               // and/or entries prefix (for example if you want to use one dictionary for multiple listeners).



            /*********************************************************************/
            // 2) listener options
            /*********************************************************************/

            /*
                because listener distribute loads among service fabric partitions of the current service
                you will need to identify the current partition id + Service name 

                or if you intent to manually map a single service fabric service partitions to event hub partition
                then you can use this CTOR, the listener will not query other partitions of the service hence it won't need a fabriClient instance
                var options = new EventHubListenerOptions(currentSFPartition);  
            */
            var currentSFPartition = ServiceInitializationParameters.PartitionId.ToString();
            var currentServiceName = ServiceInitializationParameters.ServiceName;

            // if you have restricted access to cluster then you will need a to create a fabric client (with security) and pass it to the options
            var options = new EventHubListenerOptions(currentSFPartition, currentServiceName);


            // set the processor
            options.Processor = new myEventProcessor(); // this is a class that implements IEventHubEventsProcessor


            /*
            Supported Modes:

              1- SafeDistribute: Maps 1..n Event Hub Partitions to 1 Service Fabric partition. 
                                will throw an exception if service fabric partitions are > Event hub partition
        
        
               2- Distribute: maps 1..n event hub partitions to 1 service fabric partition
                              if service fabric partitions are > event hub partitions, the remaining partitions will not
                              get any distribution (i.e Event processor will not be created on them).
        
        
        
               3- OneToOne: maps one to one event hub partition: Service Fabric Partition
                            Service Fabric partition has to = Event Hub Partitions (or an exception will be thrown)
        

        
                4- Single: maps a single event hub partition to a single service fabric partition. 
                           Event Hub communication listener will expect a supplied valid event hub partition id
            */
            options.ListenerMode = EventHubListenerMode.SafeDistribute;

            // Set the Partition State Factor
            options.StateFactory = factory;

            // Set Connection String
            options.EventHubConnectionString = mEventHubConnectionString;
            // Set Event Hub Name
            options.EventHubName = mEventHubName;

            // optionally set consumer group name (not setting it will default to "default consumer group")
            //options.EventHubConsumerGroupName = "BE01";

            /*************************************
                Addtional Options
                
                1) 
                    Use event hub Epochs (details: http://blogs.msdn.com/b/gyan/archive/2014/09/02/event-hubs-receiver-epoch.aspx)
                    options.UseEpoch(myEpochValue)



               (the below helps if you switching from receivers running elsewhere to Service Fabric)
               (details for the below: https://msdn.microsoft.com/en-us/library/azure/microsoft.servicebus.messaging.eventhubconsumergroup.createreceiver.aspx) 
               
                2) 
                    Events only newer than UTC Date
                    options.StartWithEventsNewerThan(DateTime.UtcNow)
               
                3) 
                    Specific Offset  
                    options.StartWithOffset("MyOffSet", bInclusive)             
            
            
                you can also chain the call   
                    options.UseEpoch(longEpoch)
                            .StartWithOffset(myoffset);
            **************************************/


            // override default batch size
            //options.BatchSize = 100;
            mEventHubListener = new EventHubListener(options);
            return mEventHubListener;



        }
        protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        {

            // create a new event hub listner
            return new ServiceReplicaListener[1] { new ServiceReplicaListener(p => CreateEventHubListener(), "mylistener") };
        }

        protected override async Task RunAsync(CancellationToken cancelServicePartitionReplica )
        {
            /*
                You have to wait until replica gets a write access on the state
                the safest way to do that is to start the listener (which depends on state) 
                in run async
            */
            await mEventHubListener.StartAsync();

        }

       

    }
}
