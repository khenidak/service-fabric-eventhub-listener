﻿using System;
using System.Diagnostics;
using System.Fabric;
using System.Threading;

namespace SampleStatefulSvc
{
    internal static class Program
    {
        /// <summary>
        /// This is the entry point of the service host process.
        /// </summary>
        private static void Main()
        {
            try
            {
                // Creating a FabricRuntime connects this host process to the Service Fabric runtime.
                using (FabricRuntime fabricRuntime = FabricRuntime.Create())
                {
                    // The ServiceManifest.XML file defines one or more service type names.
                    // RegisterServiceType maps a service type name to a .NET class.
                    // When Service Fabric creates an instance of this service type,
                    // an instance of the class is created in this host process.
                    fabricRuntime.RegisterServiceType("SampleStatefulSvcType", typeof(SampleStatefulSvc));

                    ServiceEventSource.Current.ServiceTypeRegistered(Process.GetCurrentProcess().Id, typeof(SampleStatefulSvc).Name);

                    Thread.Sleep(Timeout.Infinite);  // Prevents this host process from terminating to keep the service host process running.
                }
            }
            catch (Exception e)
            {
                ServiceEventSource.Current.ServiceHostInitializationFailed(e.ToString());
                throw;
            }
        }
    }
}
