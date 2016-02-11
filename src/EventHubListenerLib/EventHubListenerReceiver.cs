using EventHubListenerLib.Common;
using Microsoft.ServiceBus.Messaging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventHubListenerLib
{
    /// <summary>
    /// Creates & Wraps EventHubReceiver and controls its life cycle
    /// </summary>
    class EventHubListenerPartitionReceiver
    {
        private IEventHubPartitionState mState;
        private EventHubListenerOptions mOptions;
        private EventHubConsumerGroup mConsumerGroup;
        private string mPartitionId;

        private bool mKeepRunning = true;
        private Task mEventLoopTask;
        private bool mStarted;

        private async Task<EventHubReceiver> CreateEventHubReceiverAsync()
        {
            mState = await mOptions.StateFactory.GetOrCreateAsync(mPartitionId);


            if (null == mState && string.IsNullOrEmpty(mState.PartitionId))
                throw new InvalidOperationException("State factory returned invalid state, expected state not null && matching partition id");



            if (string.IsNullOrEmpty(mState.Offset)) // no saved state
            {
                if (!mOptions.IsOffset && !mOptions.IsStartUTCDateTime) // no starting point provided
                {
                    if (mOptions.IsEpoch) 
                        return await mConsumerGroup.CreateReceiverAsync(mPartitionId, mOptions.Epoch);
                    else
                        return await mConsumerGroup.CreateReceiverAsync(mPartitionId);
                }
                else // starting point IS PROVIDED
                {
                    if (mOptions.IsOffset) // starting point is offset
                    {
                        if (mOptions.IsEpoch)
                            return await mConsumerGroup.CreateReceiverAsync(mPartitionId, mOptions.Offset, mOptions.IsOffsetInclusive, mOptions.Epoch);
                        else
                            return await mConsumerGroup.CreateReceiverAsync(mPartitionId, mOptions.Offset, mOptions.IsOffsetInclusive);
                    }
                    else // starting point is datetime (UTC)
                    {
                        if (mOptions.IsEpoch) 
                            return await mConsumerGroup.CreateReceiverAsync(mPartitionId, mOptions.StartUTCDateTime, mOptions.Epoch);
                        else
                            return await mConsumerGroup.CreateReceiverAsync(mPartitionId, mOptions.StartUTCDateTime);

                    }
                }
            }
            else // previously saved state.
            {
                if (mOptions.IsEpoch)
                    return await mConsumerGroup.CreateReceiverAsync(mPartitionId, mState.Offset, false, mOptions.Epoch);
                else
                    return await mConsumerGroup.CreateReceiverAsync(mPartitionId, mState.Offset, false);
            }
           
        } 
        public EventHubListenerPartitionReceiver(string partitionId,
                                                EventHubListenerOptions options,
                                                EventHubConsumerGroup consumerGroup)
        {

            mPartitionId = partitionId;
            mOptions = options;
            mConsumerGroup = consumerGroup;
    
        }


        private async Task EventLoop(EventHubReceiver receiver)
        {
            mStarted = true;
            var eventsBuffer = new List<EventData>();
            var lastOffset = string.Empty;
            while (mKeepRunning)
            {
                var events = await receiver.ReceiveAsync(mOptions.BatchSize);
                foreach (var evt in events)
                {
                    lastOffset = evt.Offset;
                    eventsBuffer.Add(evt);

                    if (eventsBuffer.Count == mOptions.BatchSize)
                    {
                        var shouldSave = await mOptions.Processor.ProcessEventsAsync(events.AsEnumerable(), mState);
                        if (shouldSave)
                        {
                            mState.Offset = lastOffset;
                            await mState.SaveAsync();
                        }
                        eventsBuffer.Clear();
                    }
                }
            }
            mStarted = false;
        }


        public async Task StartAsync()
        {
            if (mStarted)
                throw new InvalidOperationException("already started");

            var eventHubReceiver = await CreateEventHubReceiverAsync();
            mEventLoopTask = EventLoop(eventHubReceiver);
        }
        
        

        public Task  StopAsync()
        {
            return Task.Run(() =>
           {

               if (!mStarted)
                   throw new InvalidOperationException("not started");

               mKeepRunning = false;
           });
        }

    }
}
